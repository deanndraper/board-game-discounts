import os
import logging
from datetime import datetime

from jinja2 import Environment, FileSystemLoader

from bgd import db

logger = logging.getLogger("bgd")


def generate(conn, config: dict):
    """Generate static HTML from active deals."""
    html_cfg = config.get("html", {})
    output_dir = html_cfg.get("output_dir", "docs")
    template_path = html_cfg.get("template", "templates/index.html.j2")
    title = html_cfg.get("title", "Board Game Deals")
    max_deals = html_cfg.get("max_deals_shown", 100)

    template_dir = os.path.dirname(template_path)
    template_file = os.path.basename(template_path)

    env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
    template = env.get_template(template_file)

    deals = db.get_active_deals_for_html(conn, limit=max_deals)
    deals_list = [dict(d) for d in deals]

    logger.info(f"Generating HTML with {len(deals_list)} active deals")

    html = template.render(
        title=title,
        deals=deals_list,
        generated_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        total_deals=len(deals_list),
    )

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "index.html")
    with open(output_file, "w") as f:
        f.write(html)

    logger.info(f"HTML written to {output_file}")
    return output_file
