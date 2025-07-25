# main.py

import os
import glob
import asyncio
import aiohttp
from typing import Optional, List, Tuple
from datetime import datetime, timedelta
from pathlib import Path
from user_input import user_input_loop

BASE_URL = "https://promo.betfair.com/betfairsp/prices/"

async def main():
    """Main function to download betting data files asynchronously."""
    country, race_type, market, start_date, end_date, combine = user_input_loop()

    # Create directory
    dir_name = create_directory_name(country, race_type, market, start_date, end_date)
    Path(dir_name).mkdir(parents=True, exist_ok=True)

    # Generate all URLs first
    filenames = generate_all_filenames(country, race_type, market, start_date, end_date)
        
    # Download all files concurrently
    await download_all_files(filenames, dir_name)

    # Combine files if requested
    if combine:
        combine_files(dir_name, dir_name)

def combine_files(dir_name: str, filename: str):
    output_file = filename + ".csv"
    csv_files = glob.glob(os.path.join(dir_name, '*.csv'))

    try:
        with open(output_file, 'w', newline='') as f_out:
            for i, file in enumerate(csv_files):
                with open(file, 'r') as f_in:
                    lines = f_in.readlines()
                    if i == 0:
                        f_out.writelines(lines)
                    else:
                        f_out.writelines(lines[1:])  # only write first header
    except Exception as e:
        print(f"❌  Error combining CSV files: {e}")
    print(f"📁  Successfully combined CSV files to {output_file}")
    
    return

def create_directory_name(country: Optional[str], race_type: str, market: str, 
                         start_date: datetime, end_date: datetime) -> str:
    """Create a standardized directory name."""
    country_part = country if country else 'all'
    race_part = "greyhound" if race_type == "g" else "horse"
    market_part = "win" if market == "w" else "place"
    
    return f"{country_part}_{race_part}_{market_part}_{start_date.date()}_{end_date.date()}"

def generate_all_filenames(country: Optional[str], race_type: str, market: str, start_date: datetime, end_date: datetime) -> List[Tuple[str, str, datetime]]:
    """Generate all file names for the date range."""
    filenames = []
    current_date = start_date
    
    while current_date <= end_date:
        filename = get_filename(race_type, market, current_date, country)
        filenames.append((filename, current_date))
        current_date += timedelta(days=1)
    
    return filenames

def get_filename(race_type: str, market: str, date: datetime, country: Optional[str]) -> Optional[str]:
    """Generate URL suffix (filename) based on race type, market, date, and country."""
    date_str = date.strftime("%d%m%Y")

    if race_type == "g":
        if market == "w":
            return f"dwbfgreyhoundwin{date_str}.csv"
        elif market == "p":
            return f"dwbfgreyhoundplace{date_str}.csv"        
    elif race_type == "h":
        if not country:
            return None
        if market == "p":
            if country == "fr":
                return f"dwbfpricesfrplaced{date_str}.csv" # fr racing is "placed" as opposed to "place" ?!?!
            else:
                return f"dwbfprices{country}place{date_str}.csv"
        elif market == "w":
            return f"dwbfprices{country}win{date_str}.csv"
    return None

async def download_all_files(urls: List[Tuple[str, datetime]], save_dir: str):
    """Download all files concurrently with progress tracking."""
    connector = aiohttp.TCPConnector(limit=3)
    timeout = aiohttp.ClientTimeout(total=60)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        semaphore = asyncio.Semaphore(2)
        
        tasks = [download_file_async(session, semaphore, filename, date, save_dir) for filename, date in urls]
        
        print(f"🚀 Starting {len(tasks)} downloads...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Summary
        successful = sum(1 for r in results if r is True)
        failed = len(results) - successful
        
        print(f"\n📊 Download Summary:")
        print(f"  ✅ Successful: {successful}")
        print(f"  ❌ Failed: {failed}")

async def download_file_async(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, filename: str, date: datetime, save_dir: str) -> bool:
    """Download a single file asynchronously with retry logic."""
    async with semaphore:  # Limit concurrent downloads
        save_path = Path(save_dir) / filename
        url = BASE_URL + filename
        
        # Retry logic for rate limiting
        max_retries = 3
        base_delay = 1.0
        
        for attempt in range(max_retries + 1):
            try:
                # Add delay between requests to be respectful
                if attempt > 0:
                    delay = base_delay * (2 ** (attempt - 1))  # Exponential backoff
                    print(f"  🔄 {date.strftime('%Y-%m-%d')}: Retry {attempt} after {delay}s")
                    await asyncio.sleep(delay)
                else:
                    # Small delay even on first attempt to spread out requests
                    await asyncio.sleep(0.1)
                
                async with session.get(url) as response:
                    if response.status == 200:
                        content = await response.read()
                        
                        # Write file
                        with open(save_path, 'wb') as file:
                            file.write(content)
                        
                        print(f"  ✅ {date.strftime('%Y-%m-%d')}: {filename}")
                        return True
                    elif response.status == 429:  # Rate limited
                        if attempt < max_retries:
                            retry_after = response.headers.get('Retry-After', '5')
                            wait_time = float(retry_after)
                            print(f"  ⏳ {date.strftime('%Y-%m-%d')}: Rate limited, waiting {wait_time}s")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            print(f"  ❌ {date.strftime('%Y-%m-%d')}: Rate limited (max retries)")
                            return False
                    else:
                        print(f"  ❌ {date.strftime('%Y-%m-%d')}: HTTP {response.status}")
                        return False
                        
            except asyncio.TimeoutError:
                if attempt < max_retries:
                    print(f"  ⏰ {date.strftime('%Y-%m-%d')}: Timeout, retrying...")
                    continue
                print(f"  ⏰ {date.strftime('%Y-%m-%d')}: Timeout (max retries)")
                return False
            except aiohttp.ClientError as e:
                if attempt < max_retries:
                    print(f"  🌐 {date.strftime('%Y-%m-%d')}: Network error, retrying...")
                    continue
                print(f"  🌐 {date.strftime('%Y-%m-%d')}: Network error - {e}")
                return False
            except IOError as e:
                print(f"  💾 {date.strftime('%Y-%m-%d')}: File error - {e}")
                return False
            except Exception as e:
                print(f"  ❓ {date.strftime('%Y-%m-%d')}: Unexpected error - {e}")
                return False
        
        return False

if __name__ == "__main__":
    asyncio.run(main())