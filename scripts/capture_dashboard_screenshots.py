from __future__ import annotations

import argparse
import time
from pathlib import Path

from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


PAGES = [
    {
        "label": "📊 Dashboard",
        "title": "📊 Pipeline Overview",
        "filename": "dashboard_overview.png",
    },
    {
        "label": "🚀 Data Generator",
        "title": "🚀 Data Generator Control",
        "filename": "dashboard_data_generator.png",
    },
    {
        "label": "⚙️ Spark Jobs",
        "title": "⚙️ Spark Jobs",
        "filename": "dashboard_spark_jobs.png",
    },
    {
        "label": "🔍 Delta Explorer",
        "title": "🔍 Delta Table Explorer",
        "filename": "dashboard_delta_explorer.png",
    },
    {
        "label": "📈 Performance",
        "title": "📈 Performance Analytics",
        "filename": "dashboard_performance.png",
    },
]


def build_driver() -> webdriver.Edge:
    options = EdgeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--hide-scrollbars")
    options.add_argument("--force-device-scale-factor=1")
    options.add_argument("--window-size=1600,1400")

    edge_paths = [
        Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
        Path(r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"),
    ]
    for edge_path in edge_paths:
        if edge_path.exists():
            options.binary_location = str(edge_path)
            break

    driver = webdriver.Edge(options=options)
    driver.set_window_size(1600, 1400)
    return driver


def xpath_literal(value: str) -> str:
    if '"' not in value:
        return f'"{value}"'
    if "'" not in value:
        return f"'{value}'"
    parts = value.split('"')
    return "concat(" + ', '.join(f'"{part}"' if part else '""' for part in parts[:-1]) + ', ' + "'\"'" + ', ' + f'"{parts[-1]}"' + ")"


def wait_for_app(driver: webdriver.Edge, timeout: int) -> None:
    wait = WebDriverWait(driver, timeout)
    wait.until(lambda current: current.execute_script("return document.readyState") == "complete")
    wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(@data-testid, 'stSidebar')]")))


def click_page(driver: webdriver.Edge, label: str, timeout: int) -> None:
    wait = WebDriverWait(driver, timeout)
    literal = xpath_literal(label)
    locator = (
        By.XPATH,
        " | ".join([
            f"//label[normalize-space()={literal}]",
            f"//*[normalize-space()={literal}]/ancestor::label[1]",
            f"//*[@role='radio']//*[normalize-space()={literal}]",
            f"//*[normalize-space()={literal}]",
        ]),
    )

    def find_clickable(current: webdriver.Edge):
        for element in current.find_elements(*locator):
            try:
                if element.is_displayed():
                    return element
            except StaleElementReferenceException:
                continue
        return False

    element = wait.until(find_clickable)
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
    time.sleep(0.5)
    driver.execute_script("arguments[0].click();", element)


def wait_for_title(driver: webdriver.Edge, title: str, timeout: int) -> None:
    wait = WebDriverWait(driver, timeout)
    literal = xpath_literal(title)
    locator = (By.XPATH, f"//*[self::h1 or self::div or self::span][normalize-space()={literal}]")
    wait.until(EC.visibility_of_element_located(locator))
    time.sleep(1.5)


def resize_for_page(driver: webdriver.Edge) -> None:
    page_height = driver.execute_script(
        "return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight, "
        "document.body.offsetHeight, document.documentElement.offsetHeight);"
    )
    target_height = max(1400, min(int(page_height) + 200, 5000))
    driver.set_window_size(1600, target_height)
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(0.5)


def capture_page(driver: webdriver.Edge, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not driver.save_screenshot(str(output_path)):
        raise RuntimeError(f"Failed to save screenshot: {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture screenshots for each Streamlit dashboard page.")
    parser.add_argument("--url", default="http://localhost:8501/", help="Dashboard URL to capture.")
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parents[1] / "docs"),
        help="Directory where PNG files should be written.",
    )
    parser.add_argument("--timeout", type=int, default=30, help="Seconds to wait for page elements.")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    driver = build_driver()

    try:
        driver.get(args.url)
        wait_for_app(driver, args.timeout)

        for page in PAGES:
            click_page(driver, page["label"], args.timeout)
            wait_for_title(driver, page["title"], args.timeout)
            resize_for_page(driver)
            capture_page(driver, output_dir / page["filename"])
            print(f"Captured {page['label']} -> {output_dir / page['filename']}")
    except TimeoutException as exc:
        raise RuntimeError(f"Timed out while capturing dashboard screenshots: {exc}") from exc
    finally:
        driver.quit()


if __name__ == "__main__":
    main()