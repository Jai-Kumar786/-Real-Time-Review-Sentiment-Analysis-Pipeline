"""
Create Visualizations from Exported Data
MLOps DataOps PoC - Visualization Script
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime
import glob

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)

# Directories
CSV_DIR = '../outputs/csv'
OUTPUT_DIR = '../outputs/screenshots'

def find_latest_export():
    """Find the most recent export file"""
    csv_files = glob.glob(f"{CSV_DIR}/reviews_export_*.csv")
    if not csv_files:
        raise FileNotFoundError("No export files found. Please run export_results.py first.")
    return max(csv_files, key=os.path.getctime)

def create_visualizations():
    """Create comprehensive visualizations"""
    try:
        # Load data
        print("Loading exported data...")
        csv_file = find_latest_export()
        df = pd.read_csv(csv_file)
        print(f"✅ Loaded {len(df)} reviews from {csv_file}")
        
        # Create output directory
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 1. Sentiment Distribution
        print("\n📊 Creating sentiment distribution chart...")
        plt.figure(figsize=(10, 6))
        sentiment_counts = df['sentiment_label'].value_counts()
        colors = {'POSITIVE': '#2ecc71', 'NEUTRAL': '#f39c12', 'NEGATIVE': '#e74c3c'}
        bars = plt.bar(sentiment_counts.index, sentiment_counts.values, 
                      color=[colors.get(x, '#95a5a6') for x in sentiment_counts.index])
        plt.title('Sentiment Distribution', fontsize=16, fontweight='bold')
        plt.xlabel('Sentiment', fontsize=12)
        plt.ylabel('Count', fontsize=12)
        plt.grid(axis='y', alpha=0.3)
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}',
                    ha='center', va='bottom', fontsize=11, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(f"{OUTPUT_DIR}/sentiment_distribution_{timestamp}.png", dpi=300, bbox_inches='tight')
        print(f"✅ Saved: sentiment_distribution_{timestamp}.png")
        plt.close()
        
        # 2. Rating vs Sentiment Score
        print("📊 Creating rating vs sentiment score chart...")
        plt.figure(figsize=(12, 6))
        for sentiment in df['sentiment_label'].unique():
            subset = df[df['sentiment_label'] == sentiment]
            plt.scatter(subset['rating'], subset['sentiment_score'], 
                       label=sentiment, alpha=0.6, s=100,
                       color=colors.get(sentiment, '#95a5a6'))
        
        plt.title('Rating vs Sentiment Score', fontsize=16, fontweight='bold')
        plt.xlabel('Rating (1-5)', fontsize=12)
        plt.ylabel('Sentiment Score', fontsize=12)
        plt.legend(title='Sentiment', fontsize=10)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(f"{OUTPUT_DIR}/rating_vs_sentiment_{timestamp}.png", dpi=300, bbox_inches='tight')
        print(f"✅ Saved: rating_vs_sentiment_{timestamp}.png")
        plt.close()
        
        # 3. Sentiment Score Distribution by Label
        print("📊 Creating sentiment score distribution chart...")
        plt.figure(figsize=(12, 6))
        for sentiment in ['POSITIVE', 'NEUTRAL', 'NEGATIVE']:
            if sentiment in df['sentiment_label'].values:
                subset = df[df['sentiment_label'] == sentiment]
                plt.hist(subset['sentiment_score'], bins=20, alpha=0.6, 
                        label=sentiment, color=colors.get(sentiment, '#95a5a6'))
        
        plt.title('Sentiment Score Distribution by Label', fontsize=16, fontweight='bold')
        plt.xlabel('Sentiment Score', fontsize=12)
        plt.ylabel('Frequency', fontsize=12)
        plt.legend(fontsize=10)
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        plt.savefig(f"{OUTPUT_DIR}/score_distribution_{timestamp}.png", dpi=300, bbox_inches='tight')
        print(f"✅ Saved: score_distribution_{timestamp}.png")
        plt.close()
        
        # 4. Processing Time Analysis
        print("📊 Creating processing time analysis chart...")
        plt.figure(figsize=(10, 6))
        plt.hist(df['processing_time_ms'], bins=30, color='#3498db', alpha=0.7, edgecolor='black')
        plt.axvline(df['processing_time_ms'].mean(), color='red', linestyle='--', 
                   linewidth=2, label=f'Mean: {df["processing_time_ms"].mean():.2f}ms')
        plt.title('Processing Time Distribution', fontsize=16, fontweight='bold')
        plt.xlabel('Processing Time (ms)', fontsize=12)
        plt.ylabel('Frequency', fontsize=12)
        plt.legend(fontsize=10)
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        plt.savefig(f"{OUTPUT_DIR}/processing_time_{timestamp}.png", dpi=300, bbox_inches='tight')
        print(f"✅ Saved: processing_time_{timestamp}.png")
        plt.close()
        
        # 5. Summary Statistics Table
        print("📊 Creating summary statistics visualization...")
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.axis('tight')
        ax.axis('off')
        
        summary_data = []
        for sentiment in ['POSITIVE', 'NEUTRAL', 'NEGATIVE']:
            if sentiment in df['sentiment_label'].values:
                subset = df[df['sentiment_label'] == sentiment]
                summary_data.append([
                    sentiment,
                    len(subset),
                    f"{len(subset)/len(df)*100:.1f}%",
                    f"{subset['sentiment_score'].mean():.3f}",
                    f"{subset['rating'].mean():.2f}",
                    f"{subset['processing_time_ms'].mean():.2f}ms"
                ])
        
        table = ax.table(cellText=summary_data,
                        colLabels=['Sentiment', 'Count', 'Percentage', 'Avg Score', 'Avg Rating', 'Avg Time'],
                        cellLoc='center',
                        loc='center',
                        colWidths=[0.15, 0.12, 0.15, 0.15, 0.15, 0.15])
        
        table.auto_set_font_size(False)
        table.set_fontsize(11)
        table.scale(1, 2)
        
        # Color header
        for i in range(6):
            table[(0, i)].set_facecolor('#3498db')
            table[(0, i)].set_text_props(weight='bold', color='white')
        
        # Color rows by sentiment
        for i, sentiment in enumerate(['POSITIVE', 'NEUTRAL', 'NEGATIVE'], 1):
            if i <= len(summary_data):
                for j in range(6):
                    table[(i, j)].set_facecolor(colors.get(sentiment, '#95a5a6'))
                    table[(i, j)].set_alpha(0.3)
        
        plt.title('Summary Statistics', fontsize=16, fontweight='bold', pad=20)
        plt.tight_layout()
        plt.savefig(f"{OUTPUT_DIR}/summary_table_{timestamp}.png", dpi=300, bbox_inches='tight')
        print(f"✅ Saved: summary_table_{timestamp}.png")
        plt.close()
        
        # Print summary
        print("\n" + "="*60)
        print("VISUALIZATION SUMMARY")
        print("="*60)
        print(f"Total Reviews Analyzed: {len(df)}")
        print(f"Visualizations Created: 5")
        print(f"Output Directory: {OUTPUT_DIR}")
        print("="*60)
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating visualizations: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("="*60)
    print("Data Visualization Script")
    print("="*60)
    create_visualizations()
