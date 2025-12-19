import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# ---------------------------
# Snowflake session (provided by Streamlit)
# ---------------------------
session = get_active_session()

# ---------------------------
# Streamlit page config
# ---------------------------
st.set_page_config(
    page_title="French Bakery Sales & Inventory Tracker",
    layout="wide"
)

st.title("🥖 French Bakery Sales & Inventory Tracker")

# ---------------------------
# Load data
# ---------------------------
@st.cache_data
def load_data():
    query = """
        SELECT SALE_ID, SALE_DATE, PRODUCT, QUANTITY, UNIT_PRICE, STOCK_QUANTITY
        FROM SALES
    """
    df = session.sql(query).to_pandas()

    # Ensure SALE_DATE is datetime
    df['SALE_DATE'] = pd.to_datetime(df['SALE_DATE'], errors='coerce')

    # Clean product names
    df['PRODUCT'] = df['PRODUCT'].astype(str).str.strip().str.upper()

    # Remove rows with null critical fields
    df = df.dropna(subset=['SALE_DATE', 'PRODUCT', 'QUANTITY', 'UNIT_PRICE'])
    return df

df = load_data()

# ---------------------------
# Sidebar Filters
# ---------------------------
st.sidebar.header("Filters")
products = sorted(df["PRODUCT"].unique())
selected_products = st.sidebar.multiselect("Select Products", products, default=products)

min_date = df["SALE_DATE"].min()
max_date = df["SALE_DATE"].max()
date_range = st.sidebar.date_input("Select Date Range", [min_date, max_date])

filtered_df = df[
    (df["PRODUCT"].isin(selected_products)) &
    (df["SALE_DATE"] >= pd.to_datetime(date_range[0])) &
    (df["SALE_DATE"] <= pd.to_datetime(date_range[1]))
]

# ---------------------------
# Display KPIs and Charts
# ---------------------------
if filtered_df.empty:
    st.warning("No data to display for the selected filters.")
else:
    # KPIs
    st.subheader("Key Metrics")
    total_revenue = (filtered_df["QUANTITY"] * filtered_df["UNIT_PRICE"]).sum()
    total_units = filtered_df["QUANTITY"].sum()
    low_stock = filtered_df[filtered_df["STOCK_QUANTITY"] < 10]["PRODUCT"].tolist()
    col1, col2, col3 = st.columns(3)
    col1.metric("💰 Total Revenue", f"€{total_revenue:,.2f}")
    col2.metric("📦 Total Units Sold", int(total_units))
    col3.metric("⚠️ Low Stock Products", ", ".join(low_stock) if low_stock else "None")

    # Daily Revenue
    st.subheader("Daily Revenue")
    daily_revenue = (
        filtered_df.groupby("SALE_DATE")
        .apply(lambda x: (x["QUANTITY"] * x["UNIT_PRICE"]).sum())
        .reset_index()
    )
    daily_revenue.columns = ["SALE_DATE", "REVENUE"]
    st.line_chart(daily_revenue.set_index("SALE_DATE")["REVENUE"])

    # Weekly Revenue
    st.subheader("Weekly Revenue Trends")
    filtered_df['WEEK'] = filtered_df['SALE_DATE'].dt.isocalendar().week
    weekly_revenue = (
        filtered_df.groupby('WEEK')
        .apply(lambda x: (x["QUANTITY"] * x["UNIT_PRICE"]).sum())
        .reset_index()
    )
    weekly_revenue.columns = ['WEEK', 'REVENUE']
    st.line_chart(weekly_revenue.set_index('WEEK')['REVENUE'])

    # Revenue by Product
    st.subheader("Revenue by Product")
    product_revenue = (
        filtered_df.groupby("PRODUCT")
        .apply(lambda x: (x["QUANTITY"] * x["UNIT_PRICE"]).sum())
        .reset_index()
    )
    product_revenue.columns = ["PRODUCT", "REVENUE"]
    product_revenue = product_revenue.sort_values("REVENUE", ascending=False)
    st.bar_chart(product_revenue.set_index("PRODUCT")["REVENUE"])

    # Top 5 Products
    st.subheader("Top 5 Products by Revenue")
    st.table(product_revenue.head(5))

    # Data Table
    st.subheader("Sales Data")
    st.dataframe(filtered_df)

    # Download Filtered Data
    st.subheader("Download Filtered Data")
    csv = filtered_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name='filtered_sales.csv',
        mime='text/csv'
    )

# ---------------------------
# Add New Sale (manual entry)
# ---------------------------
st.sidebar.header("Add New Sale")
with st.sidebar.form("add_sale_form"):
    sale_date = st.date_input("Sale Date")
    product = st.selectbox("Product", products)
    quantity = st.number_input("Quantity Sold", min_value=1)
    unit_price = st.number_input("Unit Price (€)", min_value=0.0, format="%.2f")
    submit_sale = st.form_submit_button("Add Sale")

    if submit_sale:
        session.sql(f"""
            INSERT INTO SALES (SALE_DATE, PRODUCT, QUANTITY, UNIT_PRICE, STOCK_QUANTITY)
            VALUES ('{sale_date}', '{product}', {quantity}, {unit_price}, 0)
        """).collect()
        st.success(f"Sale added for {product}! Refresh to see updates.")

# ---------------------------
# Update Inventory
# ---------------------------
st.sidebar.header("Update Inventory")
with st.sidebar.form("update_stock_form"):
    stock_product = st.selectbox("Select Product to Update Stock", products)
    stock_change = st.number_input("Change Stock Quantity (+/-)", min_value=-1000, max_value=1000, step=1)
    submit_stock = st.form_submit_button("Update Stock")

    if submit_stock:
        session.sql(f"""
            UPDATE SALES
            SET STOCK_QUANTITY = STOCK_QUANTITY + {stock_change}
            WHERE PRODUCT = '{stock_product}'
        """).collect()
        st.success(f"Stock updated for {stock_product}! Refresh to see changes.")

# ---------------------------
# Upload CSV for new sales
# ---------------------------
st.sidebar.header("Upload Sales CSV")
uploaded_file = st.sidebar.file_uploader("Upload CSV", type=['csv'])
if uploaded_file:
    new_sales = pd.read_csv(uploaded_file)

    # Clean and validate
    new_sales['SALE_DATE'] = pd.to_datetime(new_sales['SALE_DATE'], errors='coerce')
    new_sales['PRODUCT'] = new_sales['PRODUCT'].astype(str).str.strip().str.upper()
    new_sales = new_sales.dropna(subset=['SALE_DATE', 'PRODUCT', 'QUANTITY', 'UNIT_PRICE'])

    for _, row in new_sales.iterrows():
        session.sql(f"""
            INSERT INTO SALES (SALE_DATE, PRODUCT, QUANTITY, UNIT_PRICE, STOCK_QUANTITY)
            VALUES ('{row['SALE_DATE'].date()}', '{row['PRODUCT']}', {row['QUANTITY']}, {row['UNIT_PRICE']}, {row.get('STOCK_QUANTITY',0)})
        """).collect()

    st.success(f"{len(new_sales)} new sales loaded from CSV! Refresh to see updates.")


# ---------------------------
# Full Low Stock Products View
# ---------------------------
st.subheader("⚠️ Low Stock Products (Full List)")

# Aggregate stock per product
low_stock_df = (
    df.groupby("PRODUCT", as_index=False)
    .agg({"STOCK_QUANTITY": "sum"})  # Sum stock per product
)

# Filter only products with stock < 10
low_stock_df = low_stock_df[low_stock_df["STOCK_QUANTITY"] < 10]

# Sort by ascending stock
low_stock_df = low_stock_df.sort_values("STOCK_QUANTITY").reset_index(drop=True)

if low_stock_df.empty:
    st.info("All products have sufficient stock.")
else:
    st.dataframe(low_stock_df)
