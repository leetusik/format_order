import pandas as pd

ORDER_LIST_EXCEL = "통합주문리스트.xlsx"
ORDER_LIST_EXCEL_SHEET = "통합주문리스트"

df = pd.read_excel(
    ORDER_LIST_EXCEL,
    sheet_name=ORDER_LIST_EXCEL_SHEET,
)
print(df.iloc[:, [5, 9]])
