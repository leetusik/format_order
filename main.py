import pandas as pd

ORDER_LIST_EXCEL = "통합주문리스트.xlsx"
ORDER_LIST_EXCEL_SHEET = "통합주문리스트"

df = pd.read_excel(ORDER_LIST_EXCEL, sheet_name=ORDER_LIST_EXCEL_SHEET,)

# Extract only neccesary part for formatting
order_df = df.iloc[:, 5:9]

# turn no option to "NO"
option_col_name = order_df.iloc[:, 2].name
order_df[option_col_name] = order_df[option_col_name].fillna("NO")

# Replace "[쿠폰]" to ""
option_col_name = order_df.iloc[:, 1].name
order_df[option_col_name] = order_df[option_col_name].str.replace(
    "\[쿠폰\]", "", regex=True,
)

# Make 상품명구분 col
order_df["상품명구분"] = (
    order_df["판매몰상품번호/딜번호[출력]"].astype(str)
    + order_df["원상품명(쇼핑몰)[출력]"].astype(str)
    + order_df["원옵션(쇼핑몰)[출력]"]
)

# delete space
order_df["상품명구분"] = order_df["상품명구분"].str.replace(" ", "",)


MASTER_EXCEL = "쇼핑몰연동마스터.xlsx"
MASTER_EXCEL_OPTION_SHEET = "옵션분리"
MASTER_EXCEL_MASTER_SHEET = "마스터"

option_df = pd.read_excel(MASTER_EXCEL, sheet_name=MASTER_EXCEL_OPTION_SHEET, header=1)
option_df = option_df.dropna(subset=["상품명구분1"])

master_df = pd.read_excel(MASTER_EXCEL, sheet_name=MASTER_EXCEL_MASTER_SHEET, header=1)

import numpy as np

# --- Your Step 1: Initial assignment ---
# Using a temporary placeholder instead of "needed" can be clearer
# if "needed" itself could be a final desired value from option_df["옵션분리구분2"].
# Let's use a unique placeholder. If "needed" is fine, you can use that.
PLACEHOLDER = "__NEEDS_ACTUAL_VALUE__"
condition = order_df["상품명구분"].isin(option_df["상품명구분1"])
order_df["옵션분리"] = np.where(condition, PLACEHOLDER, None,)

# 2a. Create a lookup map (Series) from option_df:
#     Index = option_df["상품명구분1"]
#     Values = option_df["옵션분리구분2"]
#     Handle potential duplicates in option_df["상품명구분1"]:
#       - drop_duplicates(subset=['상품명구분1'], keep='first'): keeps the first occurrence.
#       - You could also use keep='last' or aggregate if needed, but for a simple lookup, 'first' or 'last' is common.
#       - If "상품명구분1" should be unique in option_df for this lookup, ensure that.
lookup_series = option_df.drop_duplicates(subset=["상품명구분1"], keep="first").set_index(
    "상품명구분1"
)["옵션분리구분2"]

# 2b. Identify rows in order_df that need their "옵션분리" value updated
rows_to_update_mask = order_df["옵션분리"] == PLACEHOLDER

# 2c. For these rows, get their "상품명구분" and .map() it using the lookup_series
#     .map() will return the corresponding "옵션분리구분2" value or NaN if not found in lookup_series.index
values_to_set = order_df.loc[rows_to_update_mask, "상품명구분"].map(lookup_series)

# 2d. Update order_df["옵션분리"] for the masked rows with the mapped values.
#     It's important to only assign where values_to_set is not NaN,
#     in case a "상품명구분" matched in step 1, but its corresponding "옵션분리구분2"
#     in the (de-duplicated) lookup_series is NaN or the "상품명구분1" itself was dropped due to duplication.
order_df.loc[rows_to_update_mask, "옵션분리"] = values_to_set

# (Optional) 2e. If any placeholders remain (e.g., if a mapped value was NaN),
# and you want them to be None instead of the placeholder or NaN from mapping.
# If values_to_set from map results in NaN, it will correctly place NaN.
# If the original placeholder should become None if no valid mapping occurs:
order_df.loc[order_df["옵션분리"] == PLACEHOLDER, "옵션분리"] = None


import re

new_rows_list = []
indices_to_drop_from_option_df_if_merged = []

# Create a mapping from option_df["상품명구분1"] to its index for quick lookup
# This helps find the "anchor" row in option_df
option_df_indexer = pd.Series(option_df.index, index=option_df["상품명구분1"])

# Iterate through rows in order_df that need expansion
for index, order_row in order_df.iterrows():
    if pd.notna(order_row["옵션분리"]) and order_row["옵션분리"].startswith("옵션구분"):
        try:
            # Extract N from "옵션구분N"
            num_str = re.search(r"옵션구분(\d+)", order_row["옵션분리"])
            if not num_str:
                continue  # Should not happen if starts with "옵션구분" but good practice

            total_items_n = int(num_str.group(1))
            num_rows_to_append = total_items_n - 1

            if num_rows_to_append <= 0:
                continue

            # Find the anchor row in option_df
            # This is the row in option_df that originally matched order_row["상품명구분"]
            # AND whose "옵션분리구분2" set order_row["옵션분리"]
            # For simplicity in this controlled example, we assume order_row["상품명구분"] is the primary key for this.
            # A more robust match might involve finding where option_df["상품명구분1"] == order_row["상품명구분"]
            # AND option_df["옵션분리구분2"] == order_row["옵션분리"]

            # Let's find the index in option_df that corresponds to the current order_row's "상품명구분"
            # and also where "옵션분리구분2" matches the order_row's "옵션분리"
            # This logic assumes the "옵션분리구분2" is set on the *first* item of a group in option_df

            # Find the index of the row in option_df that serves as the "anchor"
            # This is the row whose '상품명구분1' matches the order_df row's '상품명구분'
            # AND whose '옵션분리구분2' matches the order_df row's '옵션분리'

            # A robust way to find the anchor_idx:
            anchor_candidates = option_df[
                (option_df["상품명구분1"] == order_row["상품명구분"])
                & (option_df["옵션분리구분2"] == order_row["옵션분리"])
            ]

            if anchor_candidates.empty:
                print(
                    f"Warning: Could not find anchor row in option_df for order_df index {index}, 상품명구분: {order_row['상품명구분']}"
                )
                continue

            anchor_idx_in_option_df = anchor_candidates.index[0]  # Take the first match

            # Get data for the N-1 subsequent rows from option_df
            for i in range(num_rows_to_append):
                option_data_idx = (
                    anchor_idx_in_option_df + 1 + i
                )  # +1 because we want rows *after* the anchor

                if option_data_idx >= len(option_df):
                    print(
                        f"Warning: Not enough subsequent rows in option_df for order_df index {index} (needed {num_rows_to_append}, ran out at {i+1})"
                    )
                    break

                option_source_row = option_df.iloc[option_data_idx]

                new_row = {}
                # --- Populate columns for the new row ---

                # 1. Columns derived from original order_df row, possibly modified
                new_row["판매몰상품번호/딜번호[출력]"] = option_source_row["판매몰상품번호/딜번호"]
                new_row["원상품명(쇼핑몰)[출력]"] = option_source_row[
                    "원상품명_쇼핑몰"
                ]  # Usually same base product name
                # 2. Columns taken directly from the *subsequent* option_df row
                new_row["원옵션(쇼핑몰)[출력]"] = (
                    option_source_row["원옵션_쇼핑몰"]
                    if pd.notna(option_source_row["원옵션_쇼핑몰"])
                    else "NO"
                )
                new_row["수량[출력]"] = order_row[
                    "수량[출력]"
                ]  # Assume same quantity for each sub-option, adjust if needed

                # Add other mappings here:
                # new_row["order_df_col_X"] = option_source_row["option_df_col_Y"]

                # 3. Columns that are the same for the group
                new_row["옵션분리"] = order_row["옵션분리"]

                # 4. Reconstruct "상품명구분" for the new row (as per your example)
                #    Example: 2370135-1샤르망커트러리-마호가니_디너포크+나이프SET선물NO
                #    This implies it's a concatenation of the new 판매몰상품번호, original 원상품명, and *new* 원옵션.
                #    (Remove spaces/special chars as needed for your "상품명구분" logic)
                # base_for_spmg = (
                #     str(new_row["판매몰상품번호/딜번호[출력]"]) +
                #     str(new_row["원상품명(쇼핑몰)[출력]"]) +
                #     str(new_row["원옵션(쇼핑몰)[출력]"])
                # )
                # new_row["상품명구분"] = re.sub(r'[^A-Za-z0-9가-힣]', '', base_for_spmg) # Basic cleaning
                new_row["상품명구분"] = (
                    str(new_row["판매몰상품번호/딜번호[출력]"])
                    + str(new_row["원상품명(쇼핑몰)[출력]"])
                    + str(new_row["원옵션(쇼핑몰)[출력]"])
                )
                new_row["상품명구분"] = new_row["상품명구분"].replace(" ", "")

                new_rows_list.append(new_row)

        except Exception as e:
            print(f"Error processing order_df index {index}: {e}")


# Convert the list of new rows to a DataFrame
if new_rows_list:
    appended_df = pd.DataFrame(new_rows_list)
    # Ensure appended_df has the same columns as order_df, fill missing with None/NaN
    for col in order_df.columns:
        if col not in appended_df.columns:
            appended_df[col] = None
    appended_df = appended_df[order_df.columns]  # Ensure same column order

    # Concatenate with the original order_df
    final_order_df = pd.concat([order_df, appended_df], ignore_index=True)
else:
    final_order_df = order_df.copy()


# Optional: Sort to keep related items together, e.g., by the original product part of '판매몰상품번호/딜번호[출력]'
# This requires parsing the base part of the ID.
def get_base_id(prod_id):
    if isinstance(prod_id, str) and "-" in prod_id:
        return prod_id.split("-")[0]
    return prod_id


final_order_df["_sort_key"] = final_order_df["판매몰상품번호/딜번호[출력]"].apply(get_base_id)
final_order_df["_sub_sort_key"] = final_order_df["판매몰상품번호/딜번호[출력]"].apply(
    lambda x: int(x.split("-")[1]) if isinstance(x, str) and "-" in x else 0
)
final_order_df = (
    final_order_df.sort_values(by=["_sort_key", "_sub_sort_key"])
    .drop(columns=["_sort_key", "_sub_sort_key"])
    .reset_index(drop=True)
)


print("\nFinal order_df after appending rows:")
print(final_order_df)

# --- Verification for the example "2370135" ---
print("\nVerification for 2370135:")
print(final_order_df[final_order_df["원상품명(쇼핑몰)[출력]"].str.contains("샤르망")])

col_list = [
    "상품명구분1",
    "매입처",
    "상품코드",
    "상품명_ERP기준\n(빈칸삭제)",
    "옵션명_ERP기준\n(옵션공란NO채우기)",
    "상품명_발주서기준",
    "옵션명_발주서기준\n(옵션 공란 남겨두기)",
    "기준판매가",
    "매입단가",
    "단위수량",
]
master_lookup_df = master_df.drop_duplicates(subset=["상품명구분1"], keep="first")[col_list]

# 2. Perform a left merge
#    This will add '매입처' and '가격' columns from master_lookup_df to order_df
#    where '상품명구분' matches '상품명구분1'.
#    Rows in order_df without a match will get NaN in the new columns.
final_order_df = pd.merge(
    final_order_df, master_lookup_df, left_on="상품명구분", right_on="상품명구분1", how="left",
)

#      merge would create '가격_x' and '가격_y').
if "상품명구분1" in final_order_df.columns:
    final_order_df = final_order_df.drop(columns=["상품명구분1"])

final_order_df["발주수량"] = final_order_df["단위수량"] * final_order_df["수량[출력]"]
final_order_df.drop(columns="단위수량", inplace=True)
final_order_df["기준판매가합계"] = final_order_df["발주수량"] * final_order_df["기준판매가"]
final_order_df["매입가합계"] = final_order_df["발주수량"] * final_order_df["매입단가"]
final_order_df.to_csv("final_order_df.csv")
