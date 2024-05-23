'''Script to generate the Weekly Percentage Change in Stocks plot'''

def generate_weekly_winners_losers():
    import plotly.io as pio
    import os
    from jinja2 import Environment, FileSystemLoader

    from report_generate.graph import plot_weekly_percentage_change
    from report_generate.emailer import send_email


    ########Get the plot
    print("Getting the weekly percentage change plot")
    fig = plot_weekly_percentage_change()
    print("got the plot")

    # output_html_path = "output.html"
    plotly_image_path = os.path.abspath("weekly_prices.png")
    print("Converting plot to png image")
    pio.write_image(fig, plotly_image_path)
    print("Converted image to png")

    ########Convert it to a proper template form
    # Create Jinja Environment and load the template
    env = Environment(loader=FileSystemLoader(r'/opt/airflow/templates'))
    template = env.get_template(r"graph_template.html")

    print("Rendering template..")
    # Render the template with the data
    html_content = template.render(fig=plotly_image_path, myimageid='myimageid')

    print("Sending email...")
    #Send it as email
    send_email(html_content, plotly_image_path)
    print("Email sent...")

