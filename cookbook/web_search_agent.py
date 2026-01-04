"""Web Search Agent using Koala Framework.

Simple workflow:
    User Query -> Web Search -> Extract Text -> LLM Summary -> Format Output

This file contains all tool definitions that can be imported by:
- web_search_agent_airflow.py for Airflow execution
- Direct execution for local testing
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import quote_plus

try:
    from playwright.async_api import async_playwright
except ImportError:
    async_playwright = None

import re

import httpx
from lxml import html as lxml_html

from koala import LLMClient
from koala.flow import LocalExecutor, dag
from koala.tools import default_registry, tool

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def _load_env() -> None:
    """Load environment variables from .env file."""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, val = line.split("=", 1)
            os.environ[key.strip()] = val.strip().strip('"').strip("'")

    # If Playwright is unavailable, use HTTP scraping directly


@tool("web_search")
def web_search(query: str, max_results: int = 3) -> List[Dict[str, Any]]:
    """Search the web using Playwright with fallback strategy and Yahoo HTML.

    Search order: DuckDuckGo -> Google -> Yahoo (Playwright),
    Fallback: Yahoo HTML parsing (httpx + lxml) when Playwright is missing/fails.
    """

    def _yahoo_fallback() -> List[Dict[str, Any]]:
        """Yahoo search fallback via HTML parsing (no browser required)."""
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            }
            url = f"https://search.yahoo.com/search?p={quote_plus(query)}"
            with httpx.Client(timeout=10) as client:
                r = client.get(url, headers=headers)
                if r.status_code != 200:
                    return [
                        {
                            "title": "Error",
                            "url": "",
                            "content": f"Yahoo search failed: HTTP {r.status_code}",
                        }
                    ]
                doc = lxml_html.fromstring(r.text)
                items: List[Dict[str, Any]] = []

                result_nodes = doc.xpath(
                    "//div[contains(@class,'dd algo')] | //div[contains(@class,'SearchResult')]"
                )
                if not result_nodes:
                    result_nodes = doc.xpath("//ol/li | //div[contains(@class,'algo')]")

                for el in result_nodes:
                    title = "".join(
                        el.xpath(
                            ".//h3//text() | .//*[contains(@class,'title')]//text()"
                        )
                    ).strip()
                    link = (el.xpath(".//a[@href][1]/@href") or [""])[0]
                    snippet = "".join(
                        el.xpath(
                            ".//*[contains(@class,'compText') or contains(@class,'fc-14') or contains(@class,'c-title-desc')]//text()"
                        )
                    ).strip()
                    if (
                        title
                        and link.startswith("http")
                        and "yahoo.com/search" not in link
                    ):
                        items.append(
                            {
                                "title": title,
                                "url": link,
                                "content": snippet or "No description",
                            }
                        )
                        if len(items) >= max_results:
                            break

                return (
                    items
                    if items
                    else [
                        {
                            "title": "No Results",
                            "url": "",
                            "content": f"No results found for: {query}",
                        }
                    ]
                )
        except Exception as e:
            return [
                {"title": "Error", "url": "", "content": f"Yahoo fallback failed: {e}"}
            ]

    if async_playwright is None:
        return _yahoo_fallback()

    async def _search():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            results = []

            try:
                # Strategy 1: DuckDuckGo
                print(f"🔍 Searching DuckDuckGo: {query}")
                await page.goto(
                    f"https://duckduckgo.com/?q={query.replace(' ', '+')}",
                    timeout=15000,
                )
                await page.wait_for_timeout(3000)

                results = await page.evaluate(
                    f"""
                    () => {{
                        const items = [];
                        document.querySelectorAll('article[data-testid="result"], li[data-layout="organic"]').forEach(el => {{
                            const title = el.querySelector('h2, [data-testid="result-title-a"]')?.innerText?.trim();
                            const link = el.querySelector('a[href]')?.href;
                            const snippet = el.querySelector('[data-result="snippet"], .result__snippet')?.innerText?.trim();

                            if (title && link && link.startsWith('http') && !link.includes('duckduckgo.com')) {{
                                items.push({{title, url: link, content: snippet || 'No description'}});
                            }}
                        }});
                        return items.slice(0, {max_results});
                    }}
                """
                )

                if results:
                    print(f"✓ DuckDuckGo: {len(results)} results")
                    await browser.close()
                    return results

                # Strategy 2: Google fallback
                print("⚠️  DuckDuckGo: 0 results, trying Google...")
                await page.goto(
                    f"https://www.google.com/search?q={query.replace(' ', '+')}",
                    timeout=15000,
                )
                await page.wait_for_timeout(3000)

                results = await page.evaluate(
                    f"""
                    () => {{
                        const items = [];
                        document.querySelectorAll('div.g').forEach(el => {{
                            const title = el.querySelector('h3')?.innerText?.trim();
                            const link = el.querySelector('a')?.href;
                            const snippet = el.querySelector('.VwiC3b, .IsZvec')?.innerText?.trim();

                            if (title && link && link.startsWith('http') && !link.includes('google.com/search')) {{
                                items.push({{title, url: link, content: snippet || 'No description'}});
                            }}
                        }});
                        return items.slice(0, {max_results});
                    }}
                """
                )

                if results:
                    print(f"✓ Google: {len(results)} results")
                    await browser.close()
                    return results

                # Strategy 3: Yahoo final fallback
                print("⚠️  Google: 0 results, trying Yahoo...")
                await page.goto(
                    f"https://search.yahoo.com/search?p={query.replace(' ', '+')}",
                    timeout=15000,
                )
                await page.wait_for_timeout(3000)

                results = await page.evaluate(
                    f"""
                    () => {{
                        const items = [];
                        document.querySelectorAll('.dd.algo, .SearchResult').forEach(el => {{
                            const title = el.querySelector('h3, .title')?.innerText?.trim();
                            const link = el.querySelector('a[href]')?.href;
                            const snippet = el.querySelector('.compText')?.innerText?.trim();

                            if (title && link && link.startsWith('http') && !link.includes('yahoo.com/search')) {{
                                items.push({{title, url: link, content: snippet || 'No description'}});
                            }}
                        }});
                        return items.slice(0, {max_results});
                    }}
                """
                )

                if results:
                    print(f"✓ Yahoo: {len(results)} results")
                else:
                    print("⚠️  All search engines: 0 results")

            except Exception as e:
                print(f"❌ Search error: {e}")
            finally:
                await browser.close()

            return (
                results
                if results
                else [
                    {
                        "title": "No Results",
                        "url": "",
                        "content": f"No results found for: {query}",
                    }
                ]
            )

    try:
        return asyncio.run(_search())
    except Exception:
        # If Playwright is installed but browsers are missing or launch fails,
        # seamlessly fallback to Yahoo HTML parsing.
        return _yahoo_fallback()


# ============================================================================
# STEP 2: SCRAPE URL CONTENT (if needed)
# ============================================================================


@tool("scrape_urls")
def scrape_urls(
    search_results: List[Dict[str, Any]], max_pages: int = 2
) -> List[Dict[str, Any]]:
    """Visit URLs and extract actual page content.

    Uses Playwright when available; otherwise falls back to HTTP scraping
    via httpx + lxml. Also falls back to HTTP on Playwright errors.
    """

    def _http_scrape(url: str) -> str:
        """HTTP-based scraping using httpx + lxml (no browser)."""
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            }
            with httpx.Client(timeout=10) as client:
                r = client.get(url, headers=headers)
                if r.status_code != 200:
                    return ""
                doc = lxml_html.fromstring(r.text)
                for bad in doc.xpath(
                    '//script|//style|//nav|//header|//footer|//iframe|//*[contains(@class, "ad")]'
                ):
                    parent = bad.getparent()
                    if parent is not None:
                        parent.remove(bad)
                candidates = doc.xpath(
                    '//main|//article|//*[@role="main"]|//*[contains(@class,"content")]|//*[contains(@class,"main-content")]|//*[@id="content"]'
                )
                text = ""
                if candidates:
                    text = "\n".join([" ".join(el.itertext()) for el in candidates])
                else:
                    text = " ".join(doc.itertext())
                text = " ".join(text.split())
                return text[:3000]
        except Exception:
            return ""

    def _extract_text_from_html(html: str) -> str:
        """Extract readable text from already-fetched HTML using lxml.
        Removes common non-content elements and collapses whitespace.
        """
        try:
            doc = lxml_html.fromstring(html)
            for bad in doc.xpath(
                '//script|//style|//nav|//header|//footer|//iframe|//*[contains(@class, "ad")]'
            ):
                parent = bad.getparent()
                if parent is not None:
                    parent.remove(bad)
            candidates = doc.xpath(
                '//main|//article|//*[@role="main"]|//*[contains(@class,"content")]|//*[contains(@class,"main-content")]|//*[@id="content"]'
            )
            text = ""
            if candidates:
                text = "\n".join([" ".join(el.itertext()) for el in candidates])
            else:
                text = " ".join(doc.itertext())
            text = " ".join(text.split())
            return text[:3000]
        except Exception:
            return ""

    if async_playwright is None:
        enhanced = []
        for i, result in enumerate(search_results[:max_pages]):
            url = result.get("url", "")
            if url and url.startswith("http"):
                print(f"📄 HTTP scrape {i+1}/{max_pages}: {url}")
                content = _http_scrape(url)
                if content and len(content) > 100:
                    result["scraped_content"] = content
                    print(f"✓ Scraped {len(content)} chars from {url}")
            enhanced.append(result)
        return enhanced

    async def _scrape_playwright_then_http():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            enhanced_results = []
            for i, result in enumerate(search_results[:max_pages]):
                url = result.get("url", "")
                if not url or not url.startswith("http"):
                    enhanced_results.append(result)
                    continue
                try:
                    print(f"📄 Scraping {i+1}/{max_pages}: {url}")
                    await page.goto(url, timeout=10000, wait_until="domcontentloaded")
                    await page.wait_for_timeout(2000)
                    # Avoid fragile JS regex extraction; parse HTML in Python
                    html = await page.content()
                    content = _extract_text_from_html(html)
                    if content and len(content) > 100:
                        result["scraped_content"] = content
                        print(f"✓ Scraped {len(content)} chars from {url}")
                    else:
                        http_content = _http_scrape(url)
                        if http_content and len(http_content) > 100:
                            result["scraped_content"] = http_content
                            print(
                                f"✓ HTTP fallback scraped {len(http_content)} chars from {url}"
                            )
                        else:
                            print(f"⚠️  No content from {url}")
                except Exception as e:
                    print(f"❌ Failed to scrape {url} via Playwright: {e}")
                    http_content = _http_scrape(url)
                    if http_content and len(http_content) > 100:
                        result["scraped_content"] = http_content
                        print(
                            f"✓ HTTP fallback scraped {len(http_content)} chars from {url}"
                        )
                enhanced_results.append(result)
            await browser.close()
            return enhanced_results

    try:
        return asyncio.run(_scrape_playwright_then_http())
    except Exception as e:
        print(f"❌ Scraping error: {e}")
        enhanced = []
        for i, result in enumerate(search_results[:max_pages]):
            url = result.get("url", "")
            if url and url.startswith("http"):
                print(f"📄 HTTP scrape {i+1}/{max_pages}: {url}")
                content = _http_scrape(url)
                if content and len(content) > 100:
                    result["scraped_content"] = content
                    print(f"✓ Scraped {len(content)} chars from {url}")
            enhanced.append(result)
        return enhanced


# ============================================================================
# STEP 3: EXTRACT TEXT FROM RESULTS
# ============================================================================


@tool("extract_context")
def extract_context(search_results: List[Dict[str, Any]]) -> str:
    """Extract and format text from search results for LLM processing.

    Args:
        search_results: List of search result dictionaries

    Returns:
        Formatted text context
    """
    if not search_results:
        return "No search results available."

    context_parts = []
    for i, result in enumerate(search_results, 1):
        title = result.get("title", "Untitled")
        url = result.get("url", "")
        content = result.get("scraped_content") or result.get("content", "No content")

        context_parts.append(
            f"[Source {i}]\n"
            f"Title: {title}\n"
            f"URL: {url}\n"
            f"Content: {content[:1000]}...\n"
            f"{'-' * 80}"
        )

    return "\n".join(context_parts)


# ============================================================================
# STEP 4: LLM SUMMARIZATION
# ============================================================================


@tool("llm_summarize")
def llm_summarize(question: str, context: str) -> Dict[str, Any]:
    """Generate answer using LLM based on search results.

    Args:
        question: User's question
        context: Extracted search results text

    Returns:
        {answer: str, model: str, success: bool, needs_scraping: bool}
    """
    _load_env()

    def _find_gold_price(ctx: str) -> str:
        """Heuristically extract a plausible gold price from context text.
        Looks for currency-like numbers near 'gold'/'price' and returns a formatted string.
        """
        lines = [line.strip() for line in ctx.splitlines() if line.strip()]
        candidates = []
        price_regex = re.compile(r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?)")
        for line in lines:
            low = line.lower()
            if ("gold" in low and "price" in low) or ("gold" in low and "usd" in low):
                for m in price_regex.finditer(line):
                    # Normalize number
                    num_str = m.group(1).replace(",", "")
                    try:
                        val = float(num_str)
                    except ValueError:
                        continue
                    # Plausible per-ounce ranges (USD)
                    if 900 <= val <= 5000:
                        # Keep original matched representation (with $ if present)
                        full = m.group(0).strip()
                        candidates.append(full)
        # Fallback: search any numbers in context near 'gold'
        if not candidates:
            for line in lines:
                low = line.lower()
                if "gold" in low:
                    for m in price_regex.finditer(line):
                        num_str = m.group(1).replace(",", "")
                        try:
                            val = float(num_str)
                        except ValueError:
                            continue
                        if 900 <= val <= 5000:
                            candidates.append(m.group(0).strip())
        # Return the most recent/last candidate which often corresponds to current price
        return candidates[-1] if candidates else ""

    api_key = os.getenv("LLM_API_KEY")
    base_url = os.getenv("LLM_BASE_URL", "https://api.openai.com/v1")

    # If question asks for gold price, try direct extraction first
    if "gold" in question.lower() and "price" in question.lower():
        price = _find_gold_price(context)
        if price:
            # Try to capture a couple of source URLs from context
            urls = []
            for line in context.splitlines():
                if line.startswith("URL: ") and len(urls) < 2:
                    u = line.split("URL: ", 1)[1].strip()
                    if u:
                        urls.append(u)
            srcs = ", ".join(urls) if urls else "sources in search results"
            return {
                "answer": f"Approximate current gold price: {price} (USD per ounce).\nSources: {srcs}",
                "model": "heuristic",
                "success": True,
                "needs_scraping": False,
            }

    if not api_key:
        return {
            "answer": "LLM_API_KEY not configured in .env",
            "success": False,
            "needs_scraping": False,
        }

    try:
        client = LLMClient(api_key=api_key, base_url=base_url)

        system_prompt = """You are a helpful research assistant.
    Analyze the search results and provide a clear, factual answer.
    - If the search results contain actual data/facts, provide a direct answer (prefer numeric values).
    - Identify and extract prices when relevant (e.g., gold price in USD per ounce).
    - If the results only contain URLs without actual information, respond with: "NEED_URL_CONTENT".
    - Prioritize official and authoritative sources; cite sources when stating facts.
    - Be concise and direct."""

        user_prompt = f"""Question: {question}

Search Results:
{context}

Provide a clear, factual answer. If the results don't contain actual data (just URLs or descriptions), respond with "NEED_URL_CONTENT" only."""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        model = os.getenv(
            "LLM_MODEL",
            "llama-3.3-70b-versatile" if "groq" in base_url.lower() else "gpt-4o-mini",
        )
        response = client.chat(model=model, messages=messages, temperature=0.7)
        answer = response["choices"][0]["message"]["content"]

        # Check if LLM indicates need for URL scraping
        needs_scraping = "NEED_URL_CONTENT" in answer

        return {
            "answer": (
                answer
                if not needs_scraping
                else "Insufficient data in search results. Scraping URLs for more details..."
            ),
            "model": model,
            "success": True,
            "needs_scraping": needs_scraping,
        }

    except Exception as e:
        return {"answer": f"LLM error: {e}", "success": False, "needs_scraping": False}


# ============================================================================
# STEP 4: FORMAT OUTPUT
# ============================================================================


@tool("format_output")
def format_output(summary: Dict[str, Any], question: str) -> str:
    """Format the final output for display.

    Args:
        summary: LLM summary result
        question: Original question

    Returns:
        Formatted output string
    """
    output = f"\n{'=' * 80}\n"
    output += f"QUESTION: {question}\n"
    output += f"{'=' * 80}\n\n"

    if summary.get("success"):
        output += summary["answer"]
        output += f"\n\n{'─' * 80}\n"
        output += f"Model: {summary.get('model', 'Unknown')}\n"
    else:
        output += f"❌ {summary['answer']}\n"

    output += f"{'=' * 80}\n"
    return output


# ============================================================================
# WORKFLOW BUILDER
# ============================================================================


def build_web_search_flow(question: str) -> Any:
    """Build the web search workflow using Koala DAG with conditional URL scraping.

    Workflow:
        search -> scrape_urls -> extract -> summarize -> format

    Args:
        question: User's question

    Returns:
        DAGFlow object ready for execution
    """
    return (
        dag("web-search-agent")
        .step("search", "web_search", query=question, max_results=3)
        .step("scrape", "scrape_urls", search_results="$result.search", max_pages=2)
        .step("extract", "extract_context", search_results="$result.scrape")
        .step(
            "summarize", "llm_summarize", question=question, context="$result.extract"
        )
        .step("format", "format_output", summary="$result.summarize", question=question)
        .edge("search", "scrape")
        .edge("scrape", "extract")
        .edge("extract", "summarize")
        .edge("summarize", "format")
        .build()
    )


# ============================================================================
# MAIN EXECUTION (LOCAL)
# ============================================================================


def main():
    """Execute web search agent locally."""
    print("\n🔍 Web Search Agent (Koala Framework)\n")

    question = input("Enter your question (or press Enter for demo): ").strip()
    if not question:
        question = "What is the current price of gold?"
        print(f"📌 Demo question: {question}\n")

    print("🔄 Processing...\n")

    # Build workflow
    flow = build_web_search_flow(question)

    # Get tool registry
    registry = {name: meta.func for name, meta in default_registry._tools.items()}

    # Execute locally
    executor = LocalExecutor()
    results = executor.run_dagflow(flow, registry)

    # Display output
    print(results["format"])

    # Stats
    print("\n📊 Stats:")
    print(f"  • Search results: {len(results['search'])}")
    print(f"  • Context size: {len(results['extract'])} chars")
    print(f"  • LLM success: {results['summarize'].get('success', False)}")


if __name__ == "__main__":
    main()
