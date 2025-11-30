import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db
logging.basicConfig(
    filename="Logs/get_vendor_summary.log",
    level=logging.debug,
    format="%(asctime)s-%(levelname)s-%(message)s",
    filemode="a"
)


def create_vendor_summary(conn):
    ''' This Function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data ''' 
    vendor_sales_summary=pd.read_sql_query("""With FreightSummary as (
                                                select VendorNumber,
                                SUM(Freight) as FreightCost
                                From vendor_invoice
                                group by VendorNumber
                                ),
                                
                                PurchaseSummary as (Select 
                                p.VendorNumber,
                                p.VendorName,
                                p.Brand,
                                p.description,
                                p.PurchasePrice,
                                pp.Price as ActualPrice,
                                pp.Volume,
                                Sum(p.Quantity) as TotalPurchaseQuantity,
                                Sum(p.Dollars) as TotalPurchaseDollars
                                From Purchases p
                                Join Purchase_prices pp
                                On p.brand=pp.Brand
                                where p.purchasePrice>0
                                group by p.VendorNumber,p.VendorName,p.Brand,p.Description,p.PurchasePrice,pp.Price,pp.Volume
                                ),
                                
                                SalesSummary as (Select 
                                VendorNo,Brand,
                                SUM(SalesQuantity) as TotalSalesQuantity,
                                SUM(SalesDollars) as TotalSalesDollars,
                                SUM(SalesPrice) as TotalSalesPrice,
                                SUM(ExciseTax) as TotalExciseTax
                                from sales
                                group by VendorNo, Brand
                                )
                                
                                Select ps.vendorNumber,
                                ps.VendorName,
                                ps.Brand,
                                ps.Description,
                                ps.PurchasePrice,
                                ps.ActualPrice,
                                ps.Volume,
                                ps.TotalPurchaseQuantity,
                                ps.TotalPurchaseDollars,
                                ss.TotalSalesQuantity,
                                ss.TotalSalesDollars,
                                ss.TotalSalesPrice,
                                ss.TotalExciseTax,
                                fs.FreightCost
                                from
                                PurchaseSummary ps
                                Left JOIN SalesSummary ss
                                ON ps.VendorNumber=ss.VendorNo
                                AND ps.Brand=ss.Brand
                                LEFT JOIN FreightSummary fs
                                on ps.vendorNumber=fs.vendorNumber
                                Order by ps.totalPurchaseDollars DESC""",conn)

    return vendor_sales_summary

def clean_data(df):
    '''This function will clean the data '''
    #changing datatype to float
    df['Volume']=df['Volume'].astype('float')

    #Filling missing vaue with 0
    df.fillna(0,inplace=True)

    #removing spaces from categorial columns
    df['VendorName']=df['VendorName'].str.strip()
    df['description']=df['description'].str.strip()

    #creating new columns for better analysis 
    df['GrossProfit']=df['TotalSalesDollars']-df['TotalPurchaseDollars']
    df['ProfitMargin']=(df['GrossProfit']/df['TotalSalesDollars'])*100
    df['StockTurnover']=df['TotalSalesQuantity']/df['TotalPurchaseQuantity']
    df['SalestoPurchaseRatio']=df['TotalSalesDollars']/df['TotalPurchaseDollars']

    return df
    
if __name__=='__main__':
    #creating database connection
    conn=sqlite3.connect('inventory.db')
    
    logging.info('Creating Vendor Summary Table....')
    summary_df=create_vendor_summary(conn)
    logging.info(summary_df.head()) 
    
    logging.info('Cleaning_data.....')
    clean_df=clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting data....')
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logging.info("Completed")

    