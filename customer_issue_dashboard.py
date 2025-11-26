#!/usr/bin/env python3
"""
Customer Issue Dashboard V2
A web interface to view customer issues with semantic similarity insights
"""

import os
import psycopg2
import psycopg2.extras
from flask import Flask, render_template_string, jsonify
from datetime import datetime, timedelta

app = Flask(__name__)

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'email_pipeline'),
    'user': os.getenv('DB_USER', 'postgres'),
    'host': os.getenv('DB_HOST', 'localhost')
}

# HTML template
DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Customer Issue Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
            line-height: 1.6;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        h1 {
            color: #333;
            margin-bottom: 30px;
        }
        .stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .stat-value {
            font-size: 2em;
            font-weight: bold;
            color: #2563eb;
        }
        .stat-label {
            color: #666;
            margin-top: 5px;
        }
        .section {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }
        th {
            font-weight: 600;
            color: #333;
            background: #f9f9f9;
        }
        .tag {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.85em;
            font-weight: 500;
        }
        .resolved {
            background: #d4edda;
            color: #155724;
        }
        .unresolved {
            background: #f8d7da;
            color: #721c24;
        }
        .fix-instructions {
            background: #e7f3ff;
            padding: 10px;
            border-radius: 4px;
            margin-top: 10px;
            font-size: 0.9em;
            white-space: pre-wrap;
        }
        .footer {
            text-align: center;
            color: #666;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #eee;
        }
        .footer a {
            color: #2563eb;
            text-decoration: none;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Customer Issue Dashboard</h1>
        
        <div class="stats">
            <div class="stat-card">
                <div class="stat-value">{{ stats.total_issues }}</div>
                <div class="stat-label">Total Issues</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{{ stats.resolved_issues }}</div>
                <div class="stat-label">Resolved Issues</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{{ "%.1f"|format(stats.resolution_rate) }}%</div>
                <div class="stat-label">Resolution Rate</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{{ stats.unique_fixes }}</div>
                <div class="stat-label">Documented Fixes</div>
            </div>
        </div>

        <div class="section">
            <h2>Issue Categories</h2>
            <table>
                <thead>
                    <tr>
                        <th>Category</th>
                        <th>Count</th>
                        <th>Resolved</th>
                        <th>Resolution Rate</th>
                    </tr>
                </thead>
                <tbody>
                    {% for cat in categories %}
                    <tr>
                        <td>{{ cat.issue_category }}</td>
                        <td>{{ cat.count }}</td>
                        <td>{{ cat.resolved_count }}</td>
                        <td>{{ "%.1f"|format(cat.resolution_rate) }}%</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <div class="section">
            <h2>Top Issue Types</h2>
            <table>
                <thead>
                    <tr>
                        <th>Issue Type</th>
                        <th>Count</th>
                        <th>Example</th>
                    </tr>
                </thead>
                <tbody>
                    {% for issue in issue_types %}
                    <tr>
                        <td>{{ issue.issue_type }}</td>
                        <td>{{ issue.count }}</td>
                        <td>{{ issue.example_summary[:100] }}...</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <div class="section">
            <h2>Recent Issues</h2>
            <table>
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Type</th>
                        <th>Summary</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    {% for issue in recent_issues %}
                    <tr>
                        <td>{{ issue.created_at.strftime('%Y-%m-%d %H:%M') }}</td>
                        <td>{{ issue.issue_type }}</td>
                        <td>
                            {{ issue.issue_summary[:100] }}...
                            {% if issue.fix_instructions %}
                            <div class="fix-instructions">
                                <strong>Fix:</strong><br>
                                {{ issue.fix_instructions[:300] }}...
                            </div>
                            {% endif %}
                        </td>
                        <td>
                            <span class="tag {{ 'resolved' if issue.has_resolution else 'unresolved' }}">
                                {{ 'Resolved' if issue.has_resolution else 'Unresolved' }}
                            </span>
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <div class="footer">
            <p>Powered by <a href="https://applequist.com">Email Pipeline by Alec Meeker and Applequist Inc.</a></p>
            <p>Last updated: {{ datetime.now().strftime('%Y-%m-%d %H:%M:%S') }}</p>
        </div>
    </div>
</body>
</html>
"""

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(**DB_CONFIG)

@app.route('/')
def dashboard():
    """Main dashboard view"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # Get overall statistics
    cur.execute("""
        SELECT 
            COUNT(*) as total_issues,
            SUM(CASE WHEN has_resolution THEN 1 ELSE 0 END) as resolved_issues,
            SUM(CASE WHEN confidence_level = 'high' THEN 1 ELSE 0 END) as high_confidence,
            COUNT(DISTINCT CASE WHEN fix_instructions IS NOT NULL THEN id END) as unique_fixes
        FROM customer_issues_v2
    """)
    stats = cur.fetchone()
    
    resolution_rate = 0
    if stats['total_issues'] > 0:
        resolution_rate = (stats['resolved_issues'] / stats['total_issues']) * 100
    
    # Get category breakdown
    cur.execute("""
        SELECT 
            issue_category,
            COUNT(*) as count,
            SUM(CASE WHEN has_resolution THEN 1 ELSE 0 END) as resolved_count,
            ROUND(100.0 * SUM(CASE WHEN has_resolution THEN 1 ELSE 0 END) / COUNT(*), 1) as resolution_rate
        FROM customer_issues_v2
        GROUP BY issue_category
        ORDER BY count DESC
    """)
    categories = cur.fetchall()
    
    # Get top issue types
    cur.execute("""
        SELECT 
            issue_type,
            COUNT(*) as count,
            MAX(issue_summary) as example_summary
        FROM customer_issues_v2
        GROUP BY issue_type
        ORDER BY count DESC
        LIMIT 10
    """)
    issue_types = cur.fetchall()
    
    # Get recent issues
    cur.execute("""
        SELECT 
            issue_type,
            issue_category,
            issue_summary,
            has_resolution,
            fix_instructions,
            created_at
        FROM customer_issues_v2
        ORDER BY created_at DESC
        LIMIT 20
    """)
    recent_issues = cur.fetchall()
    
    conn.close()
    
    return render_template_string(
        DASHBOARD_TEMPLATE,
        stats={
            'total_issues': stats['total_issues'],
            'resolved_issues': stats['resolved_issues'],
            'resolution_rate': resolution_rate,
            'unique_fixes': stats['unique_fixes']
        },
        categories=categories,
        issue_types=issue_types,
        recent_issues=recent_issues,
        datetime=datetime
    )

@app.route('/api/stats')
def api_stats():
    """API endpoint for statistics"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # Get time-based statistics
    cur.execute("""
        SELECT 
            DATE(created_at) as date,
            COUNT(*) as issues,
            SUM(CASE WHEN has_resolution THEN 1 ELSE 0 END) as resolved
        FROM customer_issues_v2
        WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY DATE(created_at)
        ORDER BY date
    """)
    
    daily_stats = []
    for row in cur.fetchall():
        daily_stats.append({
            'date': row['date'].isoformat(),
            'issues': row['issues'],
            'resolved': row['resolved']
        })
    
    conn.close()
    
    return jsonify({
        'daily_stats': daily_stats,
        'generated_at': datetime.now().isoformat()
    })

if __name__ == '__main__':
    print("Starting Customer Issue Dashboard...")
    print("Access at: http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)