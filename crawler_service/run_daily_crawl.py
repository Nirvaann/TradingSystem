"""
Daily Crawler Runner
Runs the crawler for today's articles only and generates a summary.
Usage: python crawler_service/run_daily_crawl.py
"""
import os
import sys
import datetime
import logging

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawler_service.main import main as run_crawler
from crawler_service.utils.article_retriever import ArticleRetriever

def run_crawl_for_company(ticker):
    """
    Run the crawler for a single company ticker and return the number of articles fetched today.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print(f"\n🚀 Starting crawl for {ticker}...")
    print(f"📅 Target date: {datetime.datetime.utcnow().strftime('%Y-%m-%d')}")
    print(f"🎯 Company: {ticker}")
    print("🆓 Sources: Free news sites only\n")

    # Set the ticker as an environment variable for the crawler
    os.environ["CRAWLER_TICKER"] = ticker
    try:
        run_crawler()
    except Exception as e:
        print(f"\n❌ Crawler error for {ticker}: {e}")
        return 0

    # Retrieve today's articles for this ticker
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    retriever = ArticleRetriever()
    articles = retriever.get_articles_for_company_date(ticker, today)
    article_count = len(articles) if articles else 0

    print(f"\n✅ {ticker}: Crawl complete! {article_count} articles fetched for today.")
    return article_count


def generate_daily_summary():
    """Generate summary of today's crawl results"""
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    retriever = ArticleRetriever()
    
    print("\n" + "=" * 80)
    print(f"📊 DAILY CRAWL SUMMARY - {today}")
    print("=" * 80)
    
    companies = retriever.get_all_tracked_companies()
    total_articles = 0
    
    for ticker in companies:
        articles = retriever.get_articles_for_company_date(ticker, today)
        if articles:
            total_articles += len(articles)
            print(f"\n✅ {ticker}: {len(articles)} articles")
            for i, article in enumerate(articles[:5], 1):  # Show top 5
                print(f"   {i}. {article['title'][:70]}")
                print(f"      Mentions: {article.get('primary_company', {}).get('mentions', 0)} | "
                      f"Words: {article['word_count']} | "
                      f"Source: {article['source_domain']}")
    
    print("\n" + "=" * 80)
    print(f"📈 TOTAL: {total_articles} articles across {len(companies)} companies")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("\n🚀 Starting daily crawl...")
    print(f"📅 Target date: {datetime.datetime.utcnow().strftime('%Y-%m-%d')}")
    print("🎯 Mode: Company-specific articles only")
    print("🆓 Sources: Free news sites only\n")
    
    # Run the crawler
    try:
        run_crawler()
    except Exception as e:
        print(f"\n❌ Crawler error: {e}")
        sys.exit(1)
    
    # Generate summary
    print("\n✅ Crawl completed! Generating summary...\n")
    generate_daily_summary()