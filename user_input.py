# user_input.py

import re
from typing import Tuple, Optional, Union
from datetime import datetime, timedelta

COUNTRIES_MAP  = {
   "aus": "Australia",
   "rsa": "South Africa",
   "fr": "France",
   "usa": "United States of America",
   "uk": "United Kingdom",
   "ire": "Ireland",
   "uae": "United Arab Emirates"
}

EARLIEST_DATE = datetime(2008, 9, 2)

def user_input_loop() -> Tuple[Optional[str], str, str, datetime, datetime]:
    """
    Collect user input for data retrieval parameters.
    
    Returns:
        Tuple of (country_code, race_type, market, start_date, end_date)
    """
    print("🏇 Download Betfair directory CSV files.")
    print("=" * 45)
    
    race_type = get_race_type()
    market = get_market_type()
    country = get_country() if race_type == 'h' else None
    start_date = get_start_date()
    end_date = get_end_date(start_date)
    combine_filename = get_combine_confirmation()
    
    display_summary(country, race_type, market, start_date, end_date, combine_filename)
    
    return country, race_type, market, start_date, end_date, combine_filename


def get_race_type() -> str:
    while True:
        print("\n📋 Race Type:")
        race_type = input("Enter 'h' for horse racing or 'g' for greyhound racing: ").lower().strip()     
        if race_type in ['h', 'g']:
            return race_type
        print("❌ Invalid input. Please enter 'h' or 'g'.")

def get_market_type() -> str:
    while True:
        print("\n💰 Market Type:")
        market = input("Enter 'w' for win market or 'p' for place market: ").lower().strip()
        if market in ['w', 'p']:
            return market
        print("❌ Invalid input. Please enter 'w' or 'p'.")

def get_country() -> str:
    print("\n🌍 Available Countries:")
    for code, name in COUNTRIES_MAP.items():
        print(f"  {code:3} - {name}")

    while True:
        country = input("\nEnter the country code: ").lower().strip()
        if country in COUNTRIES_MAP:
            return country
        print(f"❌ Invalid country code. Please choose from: {', '.join(COUNTRIES_MAP.keys())}")

def get_start_date() -> datetime:
    """Get and validate start date from user."""
    min_date_str = EARLIEST_DATE.strftime("%d/%m/%Y")

    while True:
        print(f"\n📅 Start Date (must be after {min_date_str}):")
        date_str = input("Enter date in DD/MM/YYYY format: ").strip() 
        try:
            start_date = datetime.strptime(date_str, "%d/%m/%Y")           
            if start_date < EARLIEST_DATE:
                print(f"❌ Start date must be after {min_date_str}")
                continue               
            if start_date > datetime.now():
                print("❌ Start date cannot be in the future")
                continue             
            return start_date       
        except ValueError:
            print("❌ Invalid date format. Please use DD/MM/YYYY (e.g., 15/03/2023)")

def get_end_date(start_date: datetime) -> datetime:
    """Get and validate end date from user."""
    while True:
        print(f"\n📅 End Date:")
        print("  • Enter date in DD/MM/YYYY format")
        print("  • Enter 'today' for current date")
        print("  • Enter 'yesterday' for yesterday's date")
        
        date_input = input("Your choice: ").strip().lower()
        
        if date_input == "today":
            return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        elif date_input == "yesterday":
            return (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        elif date_input == "now":  # Keep backward compatibility
            return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        try:
            end_date = datetime.strptime(date_input, "%d/%m/%Y")
            if end_date > datetime.now():
                print("❌ End date cannot be in the future")
                continue       
            if end_date < start_date:
                print(f"❌ End date must be after start date ({start_date.strftime('%d/%m/%Y')})")
                continue               
            return end_date           
        except ValueError:
            print("❌ Invalid input. Please use DD/MM/YYYY format or 'today'/'yesterday'")

def get_combine_confirmation() -> bool:
    while True:
        combine_files = input("Would you like to combine the CSV files at the end of downloading? ")
        if combine_files in ['y', 'yes']:
            return True
        elif combine_files in ['n', 'no']:
            return False
        else:
            print("Please enter 'y' for yes or 'n' for no.")


def display_summary(country: Optional[str], race_type: str, market: str, start_date: datetime, end_date: datetime, combined_filename: Optional[str]) -> None:
    """Display a summary of the selected options."""
    print("\n" + "=" * 50)
    print("📊 DOWNLOAD SUMMARY")
    print("=" * 50)
    
    country_name = COUNTRIES_MAP[country] if country else "All Countries"
    race_name = "Horse Racing" if race_type == "h" else "Greyhound Racing"
    market_name = "Win Market" if market == "w" else "Place Market"
    
    date_range = f"{start_date.strftime('%d/%m/%Y')} to {end_date.strftime('%d/%m/%Y')}"
    total_days = (end_date - start_date).days + 1
    
    print(f"🌍 Country:     {country_name}")
    print(f"🏇 Race Type:   {race_name}")
    print(f"💰 Market:      {market_name}")
    print(f"📅 Date Range:  {date_range}")
    print(f"📊 Total Days:  {total_days}")
    print(f"📁 Combine files: {combined_filename}")
    print("=" * 50)
    
    while True:
        confirm = input("Proceed with download? (y/n): ").lower().strip()
        if confirm in ['y', 'yes']:
            print("🚀 Starting download...")
            break
        elif confirm in ['n', 'no']:
            print("❌ Download cancelled.")
            exit(0)
        else:
            print("Please enter 'y' for yes or 'n' for no.")