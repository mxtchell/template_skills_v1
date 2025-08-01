from datetime import datetime
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from answer_rocket import AnswerRocketClient

# Get chat_id from Node 1 (don't create new question!)
chat_id = env.chat_id
current_date = datetime.now()

# Real DDR data from analysis (hardcoded from logs)
ddr_data = [0.178, 0.202, 0.290, 0.302, 0.284, 0.216, 0.218, 0.259, 0.245, 0.208, 0.124, 0.238]
target_data = [0.239, 0.241, 0.239, 0.238, 0.239, 0.238, 0.239, 0.240, 0.238, 0.239, 0.242, 0.240]
months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

# Calculate variance and performance indicators
variance_data = [round(ddr - target, 3) for ddr, target in zip(ddr_data, target_data)]
max_value = max(max(ddr_data), max(target_data))

# Calculate chart positions for line chart (scale 0.10 to 0.35)
chart_width = 900
chart_height = 200
x_positions = [50 + (i * (chart_width - 100) / 11) for i in range(12)]
y_scale_min = 0.10
y_scale_max = 0.35
ddr_y_positions = [chart_height - ((val - y_scale_min) / (y_scale_max - y_scale_min)) * chart_height for val in ddr_data]
target_y_positions = [chart_height - ((val - y_scale_min) / (y_scale_max - y_scale_min)) * chart_height for val in target_data]
variance_y_positions = [chart_height - ((val + 0.15) / 0.25) * chart_height for val in variance_data]  # Center variance around middle

# Email setup
link = f"https://sixt.poc.answerrocket.com/apps/chat/chat-queries/{chat_id}"
sender_email = "mitchell.travis@answerrocket.com"
receiver_email = "mitchell.travis@answerrocket.com"
password = "YOUR_NEW_APP_PASSWORD"  # Replace with your new Gmail app password

# Create email
email = MIMEMultipart("related")
email["From"] = sender_email
email["To"] = receiver_email
email["Subject"] = "🚗 Sixt DDR Performance Alert - Malaga Aeropuerto Branch"

# Clean, professional Sixt newsletter design
html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sixt DDR Performance Report</title>
    <style>
        body {{ 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            margin: 0; 
            padding: 20px; 
            background-color: #f4f4f4; 
            line-height: 1.6; 
        }}
        .email-wrapper {{ 
            max-width: 1100px; 
            margin: 0 auto; 
            background-color: white; 
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 4px 20px rgba(0,0,0,0.1); 
        }}
        .header {{ 
            background: #000000; 
            padding: 30px; 
            text-align: center; 
            color: white; 
        }}
        .logo {{ 
            margin-bottom: 20px; 
        }}
        .logo img {{ 
            height: 60px; 
            width: auto; 
        }}
        .header-title {{ 
            font-size: 28px; 
            font-weight: bold; 
            margin-bottom: 8px; 
        }}
        .header-subtitle {{ 
            font-size: 16px; 
            opacity: 0.9; 
        }}
        .content {{ 
            padding: 40px; 
        }}
        .section {{ 
            margin-bottom: 40px; 
        }}
        .section-title {{ 
            font-size: 24px; 
            font-weight: bold; 
            color: #333; 
            margin-bottom: 20px; 
            border-left: 4px solid #F4811E; 
            padding-left: 16px; 
        }}
        .branch-header {{ 
            background: #F4811E; 
            color: white; 
            padding: 20px; 
            border-radius: 8px; 
            text-align: center; 
            margin-bottom: 30px; 
        }}
        .branch-name {{ 
            font-size: 32px; 
            font-weight: bold; 
            margin-bottom: 5px; 
        }}
        .branch-details {{ 
            font-size: 16px; 
            opacity: 0.9; 
        }}
        .chart-container {{ 
            background: white; 
            border: 1px solid #ddd; 
            border-radius: 8px; 
            padding: 30px; 
            margin: 20px 0; 
        }}
        .chart-title {{ 
            font-size: 20px; 
            font-weight: bold; 
            text-align: center; 
            margin-bottom: 25px; 
            color: #333; 
        }}
        .chart-legend {{ 
            text-align: center; 
            margin-bottom: 20px; 
            font-size: 14px; 
        }}
        .legend-item {{ 
            display: inline-block; 
            margin: 0 15px; 
        }}
        .legend-color {{ 
            display: inline-block; 
            width: 20px; 
            height: 3px; 
            margin-right: 6px; 
            vertical-align: middle; 
        }}
        .chart-area {{ 
            position: relative; 
            height: 350px; 
            background: #fafafa; 
            border-radius: 8px; 
            margin: 20px 0; 
            padding: 20px; 
        }}
        .chart-grid {{ 
            position: relative; 
            height: 250px; 
            width: 100%; 
            background: white; 
            border-radius: 4px; 
            border: 1px solid #ddd; 
        }}
        .y-axis {{ 
            position: absolute; 
            left: 0; 
            top: 0; 
            bottom: 0; 
            width: 50px; 
            display: flex; 
            flex-direction: column; 
            justify-content: space-between; 
            padding: 10px 5px; 
        }}
        .y-label {{ 
            font-size: 11px; 
            color: #666; 
            text-align: right; 
        }}
        .chart-content {{ 
            position: absolute; 
            left: 50px; 
            right: 10px; 
            top: 10px; 
            bottom: 40px; 
        }}
        .trend-lines {{ 
            position: relative; 
            height: 100%; 
            width: 100%; 
        }}
        .month-markers {{ 
            position: absolute; 
            bottom: -30px; 
            left: 0; 
            right: 0; 
            display: flex; 
            justify-content: space-between; 
            font-size: 11px; 
            color: #666; 
        }}
        .data-point {{ 
            position: absolute; 
            width: 8px; 
            height: 8px; 
            border-radius: 50%; 
            transform: translate(-50%, -50%); 
            border: 2px solid white; 
            box-shadow: 0 1px 3px rgba(0,0,0,0.2); 
        }}
        .point-ddr {{ background-color: #F4811E; }}
        .point-target {{ background-color: #666; }}
        .point-variance {{ background-color: #9c27b0; }}
        .chart-values {{ 
            margin-top: 15px; 
            display: flex; 
            justify-content: space-between; 
            font-size: 10px; 
        }}
        .value-column {{ 
            text-align: center; 
            flex: 1; 
        }}
        .insights {{ 
            background: #f8f9fa; 
            border-radius: 8px; 
            padding: 25px; 
            margin: 20px 0; 
        }}
        .insight-title {{ 
            font-size: 18px; 
            font-weight: bold; 
            color: #333; 
            margin-bottom: 15px; 
        }}
        .insight-content {{ 
            color: #555; 
            line-height: 1.6; 
        }}
        .highlight {{ 
            background-color: #F4811E; 
            color: white; 
            padding: 2px 8px; 
            border-radius: 4px; 
            font-weight: bold; 
        }}
        .cta {{ 
            text-align: center; 
            margin: 40px 0; 
        }}
        .cta-button {{ 
            background: linear-gradient(135deg, #F4811E 0%, #ff6b00 100%); 
            color: white; 
            padding: 15px 30px; 
            text-decoration: none; 
            border-radius: 25px; 
            font-weight: bold; 
            font-size: 16px; 
            display: inline-block; 
            box-shadow: 0 4px 15px rgba(244, 129, 30, 0.3); 
        }}
        .footer {{ 
            background: #000000; 
            color: white; 
            padding: 30px; 
            text-align: center; 
        }}
        .footer-logo {{ 
            margin-bottom: 15px; 
        }}
        .footer-text {{ 
            font-size: 14px; 
            opacity: 0.8; 
        }}
    </style>
</head>
<body>
    <div class="email-wrapper">
        <!-- Header -->
        <div class="header">
            <div class="logo">
                <img src="https://info.answerrocket.com/hubfs/poc/sixt_logo.png" alt="Sixt Logo" />
            </div>
            <div class="header-title">DDR Performance Intelligence</div>
            <div class="header-subtitle">Generated on {current_date.strftime('%B %d, %Y at %I:%M %p')}</div>
        </div>

        <!-- Content -->
        <div class="content">
            <!-- Branch Information -->
            <div class="branch-header">
                <div class="branch-name">MALAGA AEROPUERTO</div>
                <div class="branch-details">DDR1 Performance Analysis • 2019 Full Year</div>
            </div>

            <!-- Chart Section -->
            <div class="section">
                <div class="section-title">📈 Performance Overview</div>
                <div class="chart-container" style="overflow: hidden; margin-bottom: 50px;">
                    <div class="chart-title">DDR1 vs Target - Monthly Performance</div>
                    <div style="width: 100%; max-height: 450px; overflow: hidden; margin: 20px 0; padding-bottom: 30px;">
                        <img src="https://info.answerrocket.com/hubfs/poc/sixt_trend.png" alt="DDR1 vs Target - Monthly Performance" style="width: 100%; max-width: 800px; height: auto; border-radius: 8px; border: 1px solid #ddd; display: block; margin: 0 auto;" />
                    </div>
                </div>
            </div>

            <!-- Key Insights -->
            <div class="section">
                <div class="section-title">💡 Key Insights</div>
                <div class="insights">
                    <div class="insight-title">2019 Performance Analysis</div>
                    <div class="insight-content">
                        In 2019, your Malaga Aeropuerto branch demonstrated DDR performance that tracked closely with its target, though it fluctuated month to month. The DDR1 began below target in January (0.18 vs 0.24) and improved by December (0.24), reducing the gap significantly.
                        <br><br>
                        <strong>Peak Performance:</strong> April achieved exceptional results with DDR1 of <span class="highlight">0.30</span> versus target of 0.24<br>
                        <strong>Challenge Period:</strong> November showed the largest gap with DDR1 of <span class="highlight">0.12</span> vs target of 0.24<br>
                        <strong>Overall Trend:</strong> Your branch exceeded target in <span class="highlight">5 out of 12 months</span> with general improvement by year-end
                        <br><br>
                        <em>Focus areas: Investigate operational or seasonal factors contributing to November's underperformance and implement strategies from April's success across all months.</em>
                    </div>
                </div>
            </div>

            <!-- Action Items -->
            <div class="section">
                <div class="section-title">🚀 Strategic Recommendations</div>
                <div class="insights">
                    <div class="insight-title">Investigation Framework for DDR1 Underperformance</div>
                    <div class="insight-content">
                        <strong>1. Staff Experience Analysis:</strong> Review employee tenure and training status, particularly for November period. Track new employee performance and implement intensive shadowing/training for first 15 days.
                        <br><br>
                        <strong>2. Individual Performance Deep-dive:</strong> Compare DDR1 performance against other KPIs by employee. Identify high-performing staff in other metrics but low DDR1 - provide targeted coaching and visual feedback systems.
                        <br><br>
                        <strong>3. Process Engagement Review:</strong> Analyze live check-in volumes and return conversation frequency. Low engagement may indicate process gaps - implement regular monitoring and ranking systems.
                        <br><br>
                        <strong>4. Customer Feedback Integration:</strong> Review SES return inspection feedback and correlate with DDR1 performance. Poor customer feedback may indicate missed damage detection - establish feedback loops and improvement cycles.
                        <br><br>
                        <strong>5. Quality Assurance Protocol:</strong> Audit damage registration quality (image clarity, documentation completeness). Implement systematic quality checks and provide additional training where needed.
                        <br><br>
                        <em>Immediate Focus: Investigate November's significant drop (0.12 vs 0.24 target) using this framework to identify root causes and prevent recurrence.</em>
                    </div>
                </div>
            </div>

            <!-- Call to Action -->
            <div class="cta">
                <a href="{link}" class="cta-button">View Complete Analysis</a>
                <p style="margin-top: 15px; color: #666; font-size: 14px;">Access interactive charts and detailed breakdowns</p>
            </div>
        </div>

        <!-- Footer -->
        <div class="footer">
            <div class="footer-logo">
                <img src="https://info.answerrocket.com/hubfs/poc/sixt_logo.png" alt="Sixt Logo" style="height: 40px; opacity: 0.8;" />
            </div>
            <div class="footer-text">
                This automated report was generated by your Sixt DDR Intelligence System.<br>
                Powered by AnswerRocket Analytics<br><br>
                <em>Drive with confidence. Monitor with precision.</em>
            </div>
        </div>
    </div>
</body>
</html>
"""

# Attach HTML content
email.attach(MIMEText(html, "html"))

# Send the email
try:
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, email.as_string())
        print("🚗 Sixt DDR Newsletter sent successfully!")
        print(f"📧 Email sent to: {receiver_email}")
        print(f"🔗 Analysis link: {link}")
except Exception as e:
    print(f"❌ Email delivery failed: {e}")
    print("Please check your Gmail app password and try again")