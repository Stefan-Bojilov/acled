from datetime import datetime, timedelta
import os
import tempfile
from pathlib import Path

import dagster as dg
from dagster_pipeline.dagster_acled.resources.resources import PostgreSQLResource
from dagster_pipeline.dagster_acled.assets.base_assets import acled_daily_to_postgres
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pygal_maps_ua.maps import Regions
from PyPDF2 import PdfMerger


class ReportConfig(dg.Config):
    """Configuration for ACLED monthly report."""
    days_back: int = 30
    end_date: str | None = None

@dg.asset(
    name="acled_monthly_report",
    description="Generate a two-page PDF report of ACLED data quality and trends for the last month with Ukraine regional analysis",
    group_name="acled_reports", 
    deps=[acled_daily_to_postgres],
    io_manager_key="reports_s3_io_manager",  # Use the reports IO manager
)
def acled_monthly_report(
    context: dg.AssetExecutionContext,
    config: ReportConfig,
    postgres: PostgreSQLResource,
) -> str:  # Return file path, not MaterializeResult
    """Generate monthly ACLED data quality and trends report with Ukraine regional analysis as PDF."""
    # Date range using config
    if config.end_date:
        end_date = datetime.strptime(config.end_date, '%Y-%m-%d').date()
    else:
        end_date = datetime.now().date()
    
    start_date = end_date - timedelta(days=config.days_back)
    
    context.log.info(f"Generating report for period: {start_date} to {end_date}")
    
    conn = postgres.get_connection()
    
    try:
        # All your existing queries
        quality_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN event_type IS NULL OR event_type = '' THEN 1 END) as missing_event_type,
            COUNT(CASE WHEN country IS NULL OR country = '' THEN 1 END) as missing_country,
            COUNT(CASE WHEN event_date IS NULL THEN 1 END) as missing_date,
            COUNT(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 END) as missing_coordinates
        FROM acled_events_no_delete 
        WHERE event_date >= %s AND event_date <= %s
        """
        
        daily_counts_query = """
        SELECT event_date, COUNT(*) as event_count, 
               COALESCE(SUM(fatalities), 0) as total_fatalities
        FROM acled_events_no_delete 
        WHERE event_date >= %s AND event_date <= %s
        GROUP BY event_date 
        ORDER BY event_date
        """
        
        event_types_query = """
        SELECT event_type, COUNT(*) as count
        FROM acled_events_no_delete 
        WHERE event_date >= %s AND event_date <= %s
        GROUP BY event_type 
        ORDER BY count DESC
        LIMIT 10
        """
        
        region_query = """
        SELECT admin1, COUNT(*) as event_count
        FROM acled_events_no_delete 
        WHERE event_date >= %s AND event_date <= %s
          AND admin1 IS NOT NULL AND admin1 != ''
        GROUP BY admin1 
        ORDER BY event_count DESC
        """
        
        actor_query = """
        SELECT 
            COUNT(CASE WHEN actor1 IS NOT NULL AND actor1 != '' THEN 1 END) as actor1_present,
            COUNT(CASE WHEN actor2 IS NOT NULL AND actor2 != '' THEN 1 END) as actor2_present,
            COUNT(CASE WHEN disorder_type IS NOT NULL AND disorder_type != '' THEN 1 END) as disorder_type_present,
            COUNT(CASE WHEN civilian_targeting IS NOT NULL AND civilian_targeting != '' THEN 1 END) as civilian_targeting_present
        FROM acled_events_no_delete 
        WHERE event_date >= %s AND event_date <= %s
        """
        
        ukraine_region_query = """
        SELECT 
            admin1, 
            COUNT(*) as event_count,
            COALESCE(SUM(fatalities), 0) as total_fatalities,
            AVG(latitude) as avg_lat,
            AVG(longitude) as avg_lon
        FROM acled_events_no_delete 
        WHERE event_date >= %s AND event_date <= %s
          AND country = 'Ukraine'
          AND admin1 IS NOT NULL AND admin1 != ''
        GROUP BY admin1
        ORDER BY event_count DESC
        """
        
        # Execute queries
        quality_df = pd.read_sql_query(quality_query, conn, params=[start_date, end_date])
        daily_df = pd.read_sql_query(daily_counts_query, conn, params=[start_date, end_date])
        events_df = pd.read_sql_query(event_types_query, conn, params=[start_date, end_date])
        region_df = pd.read_sql_query(region_query, conn, params=[start_date, end_date])
        actor_df = pd.read_sql_query(actor_query, conn, params=[start_date, end_date])
        ukraine_df = pd.read_sql_query(ukraine_region_query, conn, params=[start_date, end_date])
        
        # Calculate all metrics
        total_records = quality_df.iloc[0]['total_records']
        critical_missing = (quality_df.iloc[0]['missing_event_type'] + 
                           quality_df.iloc[0]['missing_country'] + 
                           quality_df.iloc[0]['missing_date'] + 
                           quality_df.iloc[0]['missing_coordinates'])
        
        if total_records > 0:
            max_possible_missing = total_records * 4
            data_quality_score = ((max_possible_missing - critical_missing) / max_possible_missing) * 100
        else:
            data_quality_score = 0
        
        total_events = daily_df['event_count'].sum()
        total_fatalities = daily_df['total_fatalities'].sum()
        avg_daily_events = daily_df['event_count'].mean()
        
        ukraine_total_events = ukraine_df['event_count'].sum() if len(ukraine_df) > 0 else 0
        ukraine_total_fatalities = ukraine_df['total_fatalities'].sum() if len(ukraine_df) > 0 else 0
        ukraine_regions_with_events = len(ukraine_df) if len(ukraine_df) > 0 else 0
        
        if len(ukraine_df) > 0:
            most_active_region = ukraine_df.iloc[0]['admin1']
            most_active_count = ukraine_df.iloc[0]['event_count']
        else:
            most_active_region = "N/A"
            most_active_count = 0
        
        # Create temporary file for the report
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            temp_filepath = temp_file.name
        
        # Set up professional styling
        plt.style.use('seaborn-v0_8-whitegrid')
        plt.rcParams.update({
            'font.family': 'sans-serif',
            'font.sans-serif': ['Helvetica', 'Arial', 'DejaVu Sans'],
            'axes.spines.top': False,
            'axes.spines.right': False,
            'axes.grid': True,
            'axes.axisbelow': True,
            'grid.alpha': 0.2,
            'grid.linewidth': 0.5,
            'figure.facecolor': 'white',
            'axes.facecolor': 'white',
            'axes.edgecolor': '#CCCCCC',
            'axes.linewidth': 1,
            'xtick.color': '#333333',
            'ytick.color': '#333333',
        })
        
        # Create temp directory for intermediate files
        with tempfile.TemporaryDirectory() as temp_dir:
            page1_path = os.path.join(temp_dir, 'page1.pdf')
            page2_path = os.path.join(temp_dir, 'page2.pdf')
            
            # PAGE 1: Original Dashboard
            fig = plt.figure(figsize=(11, 8.5))
            fig.patch.set_facecolor('white')
            
            # Define professional color palette
            primary_color = '#2C3E50'
            secondary_color = '#3498DB'
            accent_color = '#E74C3C'
            success_color = '#27AE60'
            warning_color = '#F39C12'
            text_color = '#34495E'
            darker_gray = '#BDC3C7'
            
            # Create header section with gradient effect
            header_ax = fig.add_axes([0, 0.92, 1, 0.08])
            header_ax.axis('off')
            
            # Add gradient background for header
            gradient = np.linspace(0, 1, 256).reshape(1, -1)
            header_ax.imshow(gradient, extent=[0, 1, 0, 1], aspect='auto', 
                             cmap=plt.cm.Blues_r, alpha=0.15)
            
            header_ax.text(0.02, 0.5, 'ACLED INTELLIGENCE REPORT', fontsize=20, fontweight='bold', 
                          va='center', color=primary_color, fontfamily='sans-serif')
            header_ax.text(0.98, 0.5, f'{start_date.strftime("%B %d")} – {end_date.strftime("%B %d, %Y")}', 
                          fontsize=11, va='center', ha='right', color=text_color)
            
            # Add professional divider line
            header_ax.axhline(y=0.05, color=secondary_color, linewidth=3, alpha=0.8)
            
            # Key Metrics Cards
            metrics_y = 0.78
            card_height = 0.10
            
            metrics = [
                ('Total Events', f'{total_events:,}', secondary_color),
                ('Fatalities', f'{total_fatalities:,}', accent_color),
                ('Daily Average', f'{avg_daily_events:.0f}', warning_color),
                ('Data Quality', f'{data_quality_score:.1f}%', 
                 success_color if data_quality_score >= 90 else warning_color if data_quality_score >= 70 else accent_color)
            ]
            
            for i, (label, value, color) in enumerate(metrics):
                x_pos = 0.05 + i * 0.235
                card_ax = fig.add_axes([x_pos, metrics_y, 0.21, card_height])
                card_ax.axis('off')
                
                # Card with shadow effect
                shadow = mpatches.FancyBboxPatch((0.02, 0.02), 0.96, 0.96,
                                                boxstyle="round,pad=0.02",
                                                facecolor='gray', alpha=0.1,
                                                transform=card_ax.transAxes)
                card_ax.add_patch(shadow)
                
                card = mpatches.FancyBboxPatch((0, 0), 1, 1,
                                              boxstyle="round,pad=0.02",
                                              facecolor='white',
                                              edgecolor=color, linewidth=2,
                                              transform=card_ax.transAxes)
                card_ax.add_patch(card)
                
                # Icon area (colored bar on left)
                icon_bar = mpatches.Rectangle((0, 0), 0.03, 1,
                                             facecolor=color, alpha=0.8,
                                             transform=card_ax.transAxes)
                card_ax.add_patch(icon_bar)
                
                # Value and label
                card_ax.text(0.5, 0.62, value, fontsize=18, fontweight='bold',
                            ha='center', va='center', color=color)
                card_ax.text(0.5, 0.28, label.upper(), fontsize=9, 
                            ha='center', va='center', color=text_color, alpha=0.7)
            
            # Data Completeness Analysis (left upper)
            ax1 = fig.add_axes([0.05, 0.45, 0.42, 0.28])
            
            actor_completeness = {
                'Primary Actor': (actor_df.iloc[0]['actor1_present'] / total_records * 100) if total_records > 0 else 0,
                'Secondary Actor': (actor_df.iloc[0]['actor2_present'] / total_records * 100) if total_records > 0 else 0,
                'Disorder Type': (actor_df.iloc[0]['disorder_type_present'] / total_records * 100) if total_records > 0 else 0,
                'Civilian Target': (actor_df.iloc[0]['civilian_targeting_present'] / total_records * 100) if total_records > 0 else 0,
            }
            
            y_pos = np.arange(len(actor_completeness))
            values = list(actor_completeness.values())
            colors_comp = [success_color if v >= 90 else warning_color if v >= 70 else accent_color for v in values]
            
            bars = ax1.barh(y_pos, values, color=colors_comp, alpha=0.7, height=0.6)
            ax1.set_yticks(y_pos)
            ax1.set_yticklabels(list(actor_completeness.keys()), fontsize=9, color=text_color)
            ax1.set_xlabel('Data Completeness (%)', fontsize=9, color=text_color)
            ax1.set_xlim(0, 105)
            
            for i, (bar, v) in enumerate(zip(bars, values)):
                ax1.text(v + 2, bar.get_y() + bar.get_height()/2, f'{v:.1f}%', 
                        va='center', fontsize=8, color=text_color, fontweight='bold')
            
            ax1.set_title('DATA COMPLETENESS ANALYSIS', fontweight='bold', fontsize=10, 
                         color=primary_color, pad=12, loc='left')
            ax1.grid(axis='x', alpha=0.2)
            ax1.spines['left'].set_linewidth(0.5)
            ax1.spines['bottom'].set_linewidth(0.5)
            
            # Daily Activity Trends (right upper)
            ax2 = fig.add_axes([0.52, 0.45, 0.43, 0.28])
            daily_df['event_date'] = pd.to_datetime(daily_df['event_date'])
            daily_df['rolling_avg'] = daily_df['event_count'].rolling(window=7, min_periods=1).mean()
            
            ax2.fill_between(daily_df['event_date'], 0, daily_df['event_count'], 
                            color=secondary_color, alpha=0.15, label='Daily Events')
            ax2.plot(daily_df['event_date'], daily_df['event_count'], 
                    color=secondary_color, linewidth=1, alpha=0.5)
            ax2.plot(daily_df['event_date'], daily_df['rolling_avg'], 
                    color=primary_color, linewidth=2.5, label='7-Day Average')
            
            ax2.set_ylabel('Event Count', fontsize=9, color=text_color)
            ax2.set_title('DAILY ACTIVITY TRENDS', fontweight='bold', fontsize=10, 
                         color=primary_color, pad=12, loc='left')
            ax2.tick_params(colors=text_color, labelsize=8)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax2.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
            ax2.legend(loc='upper right', fontsize=8, framealpha=0.9)
            ax2.grid(True, alpha=0.2)
            ax2.spines['left'].set_linewidth(0.5)
            ax2.spines['bottom'].set_linewidth(0.5)
            
            # Event Classification (left lower)
            ax3 = fig.add_axes([0.05, 0.12, 0.42, 0.28])
            if len(events_df) > 0:
                event_labels = []
                for et in events_df['event_type']:
                    if len(et) > 25:
                        et = et[:22] + '...'
                    et = et.replace('Violence against civilians', 'Civilian Violence')
                    event_labels.append(et)
                
                colors_events = plt.cm.Blues(np.linspace(0.4, 0.8, len(events_df)))
                
                bars = ax3.barh(range(len(events_df)), events_df['count'], 
                               color=colors_events, alpha=0.8, height=0.7)
                ax3.set_yticks(range(len(events_df)))
                ax3.set_yticklabels(event_labels, fontsize=8, color=text_color)
                ax3.set_xlabel('Number of Events', fontsize=9, color=text_color)
                
                for i, v in enumerate(events_df['count']):
                    ax3.text(v + max(events_df['count']) * 0.01, i, f'{v:,}', 
                            va='center', fontsize=8, color=text_color, fontweight='bold')
            
            ax3.set_title('EVENT CLASSIFICATION', fontweight='bold', fontsize=10, 
                         color=primary_color, pad=12, loc='left')
            ax3.grid(axis='x', alpha=0.2)
            ax3.spines['left'].set_linewidth(0.5)
            ax3.spines['bottom'].set_linewidth(0.5)
            
            # Geographic Distribution (right lower)
            ax4 = fig.add_axes([0.52, 0.12, 0.43, 0.28])
            region_df_top10 = region_df.head(10)
            if len(region_df_top10) > 0:
                region_labels = []
                for region in region_df_top10['admin1']:
                    if len(region) > 20:
                        region = region[:17] + '...'
                    region_labels.append(region)
                
                colors_regions = plt.cm.Oranges(np.linspace(0.4, 0.8, len(region_df_top10)))
                
                bars = ax4.barh(range(len(region_df_top10)), region_df_top10['event_count'], 
                               color=colors_regions, alpha=0.8, height=0.7)
                ax4.set_yticks(range(len(region_df_top10)))
                ax4.set_yticklabels(region_labels, fontsize=8, color=text_color)
                ax4.set_xlabel('Number of Events', fontsize=9, color=text_color)
                
                for i, v in enumerate(region_df_top10['event_count']):
                    ax4.text(v + max(region_df_top10['event_count']) * 0.01, i, f'{v:,}', 
                            va='center', fontsize=8, color=text_color, fontweight='bold')
            
            ax4.set_title('GEOGRAPHIC DISTRIBUTION (TOP 10)', fontweight='bold', fontsize=10, 
                         color=primary_color, pad=12, loc='left')
            ax4.grid(axis='x', alpha=0.2)
            ax4.spines['left'].set_linewidth(0.5)
            ax4.spines['bottom'].set_linewidth(0.5)
            
            # Footer
            footer_ax = fig.add_axes([0, 0.02, 1, 0.04])
            footer_ax.axis('off')
            footer_ax.text(0.02, 0.5, f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")} UTC', 
                          fontsize=8, va='center', color=darker_gray, style='italic')
            footer_ax.text(0.98, 0.5, 'ACLED Data Intelligence Platform - Page 1', 
                          fontsize=8, va='center', ha='right', color=darker_gray, style='italic')
            footer_ax.axhline(y=0.9, color=darker_gray, linewidth=0.5, alpha=0.3)
            
            plt.savefig(page1_path, format='pdf', dpi=300, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            plt.close()
            
            # PAGE 2: Ukraine Regional Analysis
            fig2 = plt.figure(figsize=(11, 8.5))
            fig2.patch.set_facecolor('white')
            
            # Header for page 2
            header_ax2 = fig2.add_axes([0, 0.92, 1, 0.08])
            header_ax2.axis('off')
            gradient2 = np.linspace(0, 1, 256).reshape(1, -1)
            header_ax2.imshow(gradient2, extent=[0, 1, 0, 1], aspect='auto', 
                             cmap=plt.cm.Blues_r, alpha=0.15)
            header_ax2.text(0.02, 0.5, 'UKRAINE REGIONAL ANALYSIS', fontsize=20, fontweight='bold', 
                           va='center', color=primary_color)
            header_ax2.text(0.98, 0.5, f'{start_date.strftime("%B %d")} – {end_date.strftime("%B %d, %Y")}', 
                           fontsize=11, va='center', ha='right', color=text_color)
            header_ax2.axhline(y=0.05, color=secondary_color, linewidth=3, alpha=0.8)
            
            # Ukraine stats cards
            stats_y = 0.78
            stats_height = 0.10
            
            ukraine_stats = [
                ('Ukraine Events', f'{ukraine_total_events:,}', secondary_color),
                ('Ukraine Fatalities', f'{ukraine_total_fatalities:,}', accent_color),
                ('Active Regions', f'{ukraine_regions_with_events}', warning_color),
                ('Hottest Region', f'{most_active_region[:15]}', primary_color)
            ]
            
            for i, (label, value, color) in enumerate(ukraine_stats):
                x_pos = 0.05 + i * 0.235
                card_ax = fig2.add_axes([x_pos, stats_y, 0.21, stats_height])
                card_ax.axis('off')
                
                shadow = mpatches.FancyBboxPatch((0.02, 0.02), 0.96, 0.96,
                                                boxstyle="round,pad=0.02",
                                                facecolor='gray', alpha=0.1,
                                                transform=card_ax.transAxes)
                card_ax.add_patch(shadow)
                
                card = mpatches.FancyBboxPatch((0, 0), 1, 1,
                                              boxstyle="round,pad=0.02",
                                              facecolor='white',
                                              edgecolor=color, linewidth=2,
                                              transform=card_ax.transAxes)
                card_ax.add_patch(card)
                
                icon_bar = mpatches.Rectangle((0, 0), 0.03, 1,
                                             facecolor=color, alpha=0.8,
                                             transform=card_ax.transAxes)
                card_ax.add_patch(icon_bar)
                
                card_ax.text(0.5, 0.62, value, fontsize=16, fontweight='bold',
                            ha='center', va='center', color=color)
                card_ax.text(0.5, 0.28, label.upper(), fontsize=8, 
                            ha='center', va='center', color=text_color, alpha=0.7)
            
            # Ukraine visualizations
            if len(ukraine_df) > 0:
                # Left panel: Horizontal bar chart
                ax_left = fig2.add_axes([0.05, 0.15, 0.44, 0.55])
                ukraine_df_top = ukraine_df.head(15)
                norm = plt.Normalize(vmin=ukraine_df_top['event_count'].min(), 
                                   vmax=ukraine_df_top['event_count'].max())
                colors_ukraine = plt.cm.YlOrRd(norm(ukraine_df_top['event_count']))
                ukraine_df_display = ukraine_df_top.sort_values('event_count', ascending=True)
                
                bars = ax_left.barh(range(len(ukraine_df_display)), 
                                   ukraine_df_display['event_count'],
                                   color=colors_ukraine[::-1], alpha=0.8, height=0.7)
                
                ax_left.set_yticks(range(len(ukraine_df_display)))
                ax_left.set_yticklabels(ukraine_df_display['admin1'], fontsize=9, color=text_color)
                ax_left.set_xlabel('Number of Events', fontsize=10, color=text_color)
                ax_left.set_title('TOP 15 REGIONS BY EVENT COUNT', fontweight='bold', 
                                fontsize=11, color=primary_color, pad=15)
                
                for i, (idx, row) in enumerate(ukraine_df_display.iterrows()):
                    ax_left.text(row['event_count'] + max(ukraine_df_display['event_count']) * 0.01, 
                               i, f"{row['event_count']:,}", 
                               va='center', fontsize=8, color=text_color, fontweight='bold')
                
                ax_left.grid(axis='x', alpha=0.2)
                ax_left.spines['top'].set_visible(False)
                ax_left.spines['right'].set_visible(False)
                
                # Right panel: Bubble chart
                ax_right = fig2.add_axes([0.54, 0.15, 0.41, 0.55])
                bubble_data = ukraine_df.head(20).copy()
                
                max_fatalities = bubble_data['total_fatalities'].max()
                if max_fatalities > 0:
                    bubble_sizes = 50 + (bubble_data['total_fatalities'] / max_fatalities) * 500
                else:
                    bubble_sizes = [100] * len(bubble_data)
                
                scatter = ax_right.scatter(bubble_data['event_count'], 
                                          range(len(bubble_data)),
                                          s=bubble_sizes,
                                          c=bubble_data['event_count'],
                                          cmap='YlOrRd',
                                          alpha=0.6,
                                          edgecolors='black',
                                          linewidth=0.5)
                
                for i, (idx, row) in enumerate(bubble_data.iterrows()):
                    ax_right.text(row['event_count'], i, 
                                row['admin1'][:12], 
                                ha='center', va='center',
                                fontsize=7, color='black',
                                fontweight='bold')
                
                ax_right.set_xlabel('Event Count', fontsize=10, color=text_color)
                ax_right.set_ylabel('Region Rank', fontsize=10, color=text_color)
                ax_right.set_title('EVENT INTENSITY & FATALITIES', fontweight='bold', 
                                 fontsize=11, color=primary_color, pad=15)
                ax_right.set_ylim(-1, len(bubble_data))
                ax_right.grid(True, alpha=0.2)
                ax_right.spines['top'].set_visible(False)
                ax_right.spines['right'].set_visible(False)
                
                legend_elements = [
                    plt.scatter([], [], s=100, c='gray', alpha=0.6, label='Low Fatalities'),
                    plt.scatter([], [], s=300, c='gray', alpha=0.6, label='Medium Fatalities'),
                    plt.scatter([], [], s=500, c='gray', alpha=0.6, label='High Fatalities')
                ]
                ax_right.legend(handles=legend_elements, loc='upper right', fontsize=8)
                
            else:
                no_data_ax = fig2.add_axes([0.1, 0.3, 0.8, 0.4])
                no_data_ax.text(0.5, 0.5, 'No Ukraine data available for this period', 
                              ha='center', va='center', fontsize=16, color=text_color)
                no_data_ax.axis('off')
            
            # Footer for page 2
            footer_ax2 = fig2.add_axes([0, 0.02, 1, 0.04])
            footer_ax2.axis('off')
            footer_ax2.text(0.02, 0.5, f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")} UTC', 
                           fontsize=8, va='center', color=darker_gray, style='italic')
            footer_ax2.text(0.98, 0.5, 'ACLED Data Intelligence Platform - Page 2', 
                           fontsize=8, va='center', ha='right', color=darker_gray, style='italic')
            footer_ax2.axhline(y=0.9, color=darker_gray, linewidth=0.5, alpha=0.3)
            
            plt.savefig(page2_path, format='pdf', dpi=300, bbox_inches='tight',
                       facecolor='white', edgecolor='none')
            plt.close()
            
            # Merge PDFs
            merger = PdfMerger()
            merger.append(page1_path)
            merger.append(page2_path)
            merger.write(temp_filepath)
            merger.close()
        
        context.log.info(f"Report generated at: {temp_filepath}")
        
        # Add metadata using context instead of MaterializeResult
        context.add_output_metadata({
            "period_start": start_date.isoformat(),
            "period_end": end_date.isoformat(),
            "total_records": int(total_records),
            "total_events": int(total_events),
            "total_fatalities": int(total_fatalities),
            "data_quality_score": float(data_quality_score),
            "regions_covered": len(region_df),
            "event_types": len(events_df),
            "ukraine_total_events": int(ukraine_total_events),
            "ukraine_total_fatalities": int(ukraine_total_fatalities),
            "ukraine_regions_with_activity": int(ukraine_regions_with_events),
        })
        
        # Return the file path - IO manager will handle S3 upload
        return temp_filepath
        
    except Exception as e:
        context.log.error(f"Error generating report: {str(e)}")
        raise
    finally:
        conn.close()


@dg.asset(
    name="UA_event_heatmap",
    group_name="acled_reports", 
    deps=[acled_daily_to_postgres],
    io_manager_key="reports_s3_io_manager",  # Use the reports IO manager
)
def acled_ua_heatmap(
    context: dg.AssetExecutionContext,
    config: ReportConfig,
    postgres: PostgreSQLResource,
) -> str:  # Return file path, not MaterializeResult
    """Generate Ukraine regional heatmap."""
    os.environ['DYLD_LIBRARY_PATH'] = '/opt/homebrew/lib:' + os.environ.get('DYLD_LIBRARY_PATH', '')

    if config.end_date:
        end_date = datetime.strptime(config.end_date, '%Y-%m-%d').date()
    else:
        end_date = datetime.now().date()
    
    start_date = end_date - timedelta(days=config.days_back)
    
    context.log.info(f"Generating heatmap for period: {start_date} to {end_date}")
    
    conn = postgres.get_connection()

    try:
        region_query = """
            SELECT admin1, COUNT(*) as event_count
            FROM acled_events_no_delete 
            WHERE event_date >= %s AND event_date <= %s
              AND admin1 IS NOT NULL AND admin1 != ''
            GROUP BY admin1 
            ORDER BY event_count DESC
            """

        data = pd.read_sql_query(region_query, conn, params=[start_date, end_date])
        event_data = data.set_index('admin1')['event_count'].to_dict()
        
        region_mapping = {
            'Donetsk': 'donetsk',
            'Kherson': 'kherson',
            'Kharkiv': 'kharkiv',
            'Sumy': 'sumy',
            'Zaporizhia': 'zaporizhzhia', 
            'Dnipropetrovsk': 'dnipropetrovsk',
            'Chernihiv': 'chernihiv',
            'Mykolaiv': 'mykolaiv',
            'Luhansk': 'luhansk',
            'Crimea': 'crimea',
            'Odesa': 'odesa',
            'Kyiv City': 'kyivcity',
            'Cherkasy': 'cherkasy',
            'Khmelnytskyi': 'khmelnitskyi',
            'Kyiv': 'kyiv',
            'Ternopil': 'ternopil',
            'Vinnytsia': 'vinnytsia',
            'Lviv': 'lviv',
            'Volyn': 'volyn',
            'Zakarpattia': 'zakarpattia',
            'Ivano-Frankivsk': 'ivano-frankivsk',
            'Kirovohrad': 'kirovohrad',
            'Rivne': 'rivne',
            'Poltava': 'poltava',
            'Zhytomyr': 'zhytomir'
        }

        mapped_event_data = {
            region_mapping[region]: count 
            for region, count in event_data.items() 
            if region in region_mapping
        }

        # Create temporary file for the heatmap
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as temp_file:
            temp_heatmap_path = temp_file.name
        
        # Generate heatmap
        map = Regions(legend_at_bottom=True)
        map.title = f'Event Distribution by Ukrainian Region ({start_date} to {end_date})'
        map.add('Event Count', mapped_event_data)
        map.render_to_png(temp_heatmap_path)
        
        context.log.info(f"Heatmap generated at: {temp_heatmap_path}")
        
        # Add metadata using context
        context.add_output_metadata({
            "period_start": start_date.isoformat(),
            "period_end": end_date.isoformat(),
            "regions_with_data": len(mapped_event_data),
            "total_events": sum(mapped_event_data.values()),
        })
        
        # Return the file path - IO manager will handle S3 upload
        return temp_heatmap_path
        
    except Exception as e:
        context.log.error(f"Error generating heatmap: {str(e)}")
        raise
    finally:
        conn.close()