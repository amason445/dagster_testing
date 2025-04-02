import dagster as dg
import pandas as pd

from helpers import utilities

@dg.asset
def basic_facts():
    #read sheet from scrape called basic facts
    df = pd.read_excel("data/regional_banks.xlsx", sheet_name= 'Basic Facts')

    #clean df
    df['Company Name'] = df['Company Name'].apply(lambda x: str(x).replace('Inc', ''))
    df['Company Name'] = df['Company Name'].apply(lambda x: str(x).replace('Corp', ''))
    df['Company Name'] = df['Company Name'].apply(lambda x: str(x).replace('Co', ''))
    df['Security Price'] = df['Security Price'].astype(float).fillna(0)
    df['Volume (90 Day Avg)'] = df['Volume (90 Day Avg)'].astype(float).fillna(0)
    df['Market Capitalization'] = df['Market Capitalization'].astype(str).apply(lambda x: utilities.clean_market_cap(x)).astype(float)
    df['Dividend Yield'] = df['Dividend Yield'].astype(float).fillna(0)
    df['Optionable'] = df['Optionable'].apply(lambda x: str(x).replace('(Option Chain)', ''))

    df.to_csv("data/processed_basic.csv", index= False)
    return "Data processed successfully"

@dg.asset
def performance_volatility():
    #read sheet from scrape called basic facts
    df = pd.read_excel("data/regional_banks.xlsx", sheet_name= 'Performance & Volatility')

    #clean df
    df['Company Name'] = df['Company Name'].apply(lambda x: str(x).replace('Inc', ''))
    df['Company Name'] = df['Company Name'].apply(lambda x: str(x).replace('Corp', ''))
    df['Company Name'] = df['Company Name'].apply(lambda x: str(x).replace('Co', ''))
    df['Price Performance (52 Weeks)'] = df['Price Performance (52 Weeks)'].astype(float).fillna(0)
    df['Total Return (1 Yr Annualized)'] = df['Total Return (1 Yr Annualized)'].astype(float).fillna(0)
    df['Beta (1 Year Annualized)'] = df['Beta (1 Year Annualized)'].astype(float).fillna(0)
    df['Standard Deviation (1 Yr Annualized)'] = df['Standard Deviation (1 Yr Annualized)'].astype(float).fillna(0)

    df.to_csv("data/processed_perf.csv", index= False)
    return "Data processed successfully"

@dg.asset
def valuation():
    #read sheet from scrape called basic facts
    df = pd.read_excel("data/regional_banks.xlsx", sheet_name= 'Valuation, Growth & Ownership')

    #clean df
    df['Company Name'] = df['Company Name'].apply(lambda x: str(x).replace('Inc', ''))
    df['Company Name'] = df['Company Name'].apply(lambda x: str(x).replace('Corp', ''))
    df['Company Name'] = df['Company Name'].apply(lambda x: str(x).replace('Co', ''))
    df = df.drop(columns=['S&P Global Market Intelligence Valuation', 'S&P Global Market Intelligence Quality', 
                  'S&P Global Market Intelligence Growth Stability', 'S&P Global Market Intelligence Financial Health'])
    df['P/E (Price/TTM Earnings)'] = df['P/E (Price/TTM Earnings)'].apply(lambda x: str(x).replace('--', '0')).astype(float)
    df['PEG Ratio'] = df['PEG Ratio'].astype(float).fillna(0)
    df['EPS Growth (Proj This Yr vs. Last Yr)'] = df['EPS Growth (Proj This Yr vs. Last Yr)'].astype(float).fillna(0)
    df['Institutional Ownership'] = df['Institutional Ownership'].astype(float).fillna(0)
    df['Institutional Ownership (Last vs. Prior Qtr)'] = df['Institutional Ownership (Last vs. Prior Qtr)'].astype(float).fillna(0)
    
    df.to_csv("data/processed_valuation.csv", index= False)
    return "Data processed successfully"

@dg.asset(deps=[basic_facts, performance_volatility, valuation])
def merged_dataset():
    df_basic = pd.read_csv("data/processed_basic.csv")
    df_perf = pd.read_csv("data/processed_perf.csv")
    df_val = pd.read_csv("data/processed_valuation.csv")

    # Merge step — assuming all share 'Company Name' as the join key
    merged_df = df_basic.merge(df_perf, on=["Symbol", "Company Name"], how="left") \
                        .merge(df_val, on=["Symbol", "Company Name"], how="left")

    merged_df.to_csv("data/final_merged.csv", index=False)
    return "Data has been successfully merged"


defs = dg.Definitions(assets=[basic_facts, performance_volatility, valuation, merged_dataset])
    

