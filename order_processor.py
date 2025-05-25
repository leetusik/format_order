import logging
import re
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OrderProcessor:
    """
    A class to process order data by merging with option and master data.

    This processor handles:
    1. Loading and cleaning order data
    2. Option separation and expansion
    3. Master data integration
    4. Final calculations and output
    """

    def __init__(
        self,
        order_excel_path: str = "통합주문리스트.xlsx",
        master_excel_path: str = "쇼핑몰연동마스터.xlsx",
    ):
        """
        Initialize the OrderProcessor with file paths.

        Args:
            order_excel_path: Path to the order list Excel file
            master_excel_path: Path to the master data Excel file
        """
        self.order_excel_path = order_excel_path
        self.master_excel_path = master_excel_path
        self.order_df = None
        self.option_df = None
        self.master_df = None
        self.final_order_df = None

    def load_data(self) -> None:
        """Load all required data from Excel files."""
        try:
            # Load order data
            logger.info("Loading order data...")
            try:
                df = pd.read_excel(self.order_excel_path, sheet_name="통합주문리스트")
            except ValueError:
                # If sheet not found, use the first sheet
                logger.info("Sheet '통합주문리스트' not found, using first sheet")
                df = pd.read_excel(self.order_excel_path, sheet_name=0)
                # df.to_excel("order_data.xlsx", index=False)
            self.order_df = df.iloc[:, 5:9].copy()

            # Load option data
            logger.info("Loading option data...")
            try:
                self.option_df = pd.read_excel(
                    self.master_excel_path, sheet_name="옵션분리", header=1
                ).dropna(subset=["상품명구분1"])
            except ValueError:
                # If sheet not found, try to find a sheet with similar name or use first sheet
                logger.info("Sheet '옵션분리' not found, using first sheet")
                self.option_df = pd.read_excel(
                    self.master_excel_path, sheet_name=0, header=1
                ).dropna(subset=["상품명구분1"])
            # self.option_df.to_excel("option_data.xlsx", index=False)

            # Load master data
            logger.info("Loading master data...")
            try:
                self.master_df = pd.read_excel(
                    self.master_excel_path, sheet_name="마스터", header=1
                )

            except ValueError:
                # If sheet not found, try to find a sheet with similar name or use second sheet if available
                logger.info(
                    "Sheet '마스터' not found, trying second sheet or first sheet"
                )
                excel_file = pd.ExcelFile(self.master_excel_path)
                if len(excel_file.sheet_names) > 1:
                    self.master_df = pd.read_excel(
                        self.master_excel_path, sheet_name=1, header=1
                    )
                else:
                    self.master_df = pd.read_excel(
                        self.master_excel_path, sheet_name=0, header=1
                    )
            # self.master_df.to_excel("master_data.xlsx", index=False)

            logger.info("Data loading completed successfully")

        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    def clean_order_data(self) -> None:
        """Clean and prepare order data."""
        if self.order_df is None:
            raise ValueError("Order data not loaded. Call load_data() first.")

        logger.info("Cleaning order data...")

        # Fill missing options with "NO"
        option_col_name = self.order_df.iloc[:, 2].name
        self.order_df[option_col_name] = self.order_df[option_col_name].fillna("NO")

        # Remove "[쿠폰]" from product names
        product_col_name = self.order_df.iloc[:, 1].name
        self.order_df[product_col_name] = self.order_df[product_col_name].str.replace(
            r"\[쿠폰\]", "", regex=True
        )

        # Create product identifier column
        self._create_product_identifier()

        logger.info("Order data cleaning completed")

    def _create_product_identifier(self) -> None:
        """Create a unique product identifier by combining relevant columns."""
        self.order_df["상품명구분"] = (
            self.order_df["판매몰상품번호/딜번호[출력]"].astype(str)
            + self.order_df["원상품명(쇼핑몰)[출력]"].astype(str)
            + self.order_df["원옵션(쇼핑몰)[출력]"]
        )

        # Remove spaces from identifier
        self.order_df["상품명구분"] = self.order_df["상품명구분"].str.replace(" ", "")

    def process_option_separation(self) -> None:
        """Process option separation logic."""
        if self.order_df is None or self.option_df is None:
            raise ValueError("Data not loaded. Call load_data() first.")

        logger.info("Processing option separation...")

        # Step 1: Initial assignment with placeholder
        placeholder = "__NEEDS_ACTUAL_VALUE__"
        condition = self.order_df["상품명구분"].isin(self.option_df["상품명구분1"])
        self.order_df["옵션분리"] = np.where(condition, placeholder, None)

        # Step 2: Create lookup and update values
        self._update_option_separation_values(placeholder)

        logger.info("Option separation processing completed")

    def _update_option_separation_values(self, placeholder: str) -> None:
        """Update option separation values using lookup from option_df."""
        # Create lookup series
        lookup_series = self.option_df.drop_duplicates(
            subset=["상품명구분1"], keep="first"
        ).set_index("상품명구분1")["옵션분리구분2"]

        # Update values
        rows_to_update_mask = self.order_df["옵션분리"] == placeholder
        values_to_set = self.order_df.loc[rows_to_update_mask, "상품명구분"].map(
            lookup_series
        )
        self.order_df.loc[rows_to_update_mask, "옵션분리"] = values_to_set

        # Clean up remaining placeholders
        self.order_df.loc[self.order_df["옵션분리"] == placeholder, "옵션분리"] = None

    def expand_option_rows(self) -> None:
        """Expand rows based on option separation requirements."""
        if self.order_df is None or self.option_df is None:
            raise ValueError("Data not loaded. Call load_data() first.")

        logger.info("Expanding option rows...")

        new_rows_list = []

        for index, order_row in self.order_df.iterrows():
            if self._should_expand_row(order_row):
                expanded_rows = self._create_expanded_rows(order_row)
                new_rows_list.extend(expanded_rows)

        # Combine original and new rows
        self._combine_rows(new_rows_list)

        logger.info(f"Added {len(new_rows_list)} expanded rows")

    def _should_expand_row(self, row: pd.Series) -> bool:
        """Check if a row should be expanded based on option separation."""
        return (
            pd.notna(row["옵션분리"])
            and isinstance(row["옵션분리"], str)
            and row["옵션분리"].startswith("옵션구분")
        )

    def _create_expanded_rows(self, order_row: pd.Series) -> List[Dict]:
        """Create expanded rows for a given order row."""
        new_rows = []

        try:
            # Extract number from "옵션구분N"
            num_match = re.search(r"옵션구분(\d+)", order_row["옵션분리"])
            if not num_match:
                return new_rows

            total_items = int(num_match.group(1))
            num_rows_to_append = total_items - 1

            if num_rows_to_append <= 0:
                return new_rows

            # Find anchor row in option_df
            anchor_idx = self._find_anchor_index(order_row)
            if anchor_idx is None:
                return new_rows

            # Create new rows
            for i in range(num_rows_to_append):
                option_data_idx = anchor_idx + 1 + i

                if option_data_idx >= len(self.option_df):
                    logger.warning(f"Not enough subsequent rows in option_df")
                    break

                new_row = self._create_new_row(order_row, option_data_idx)
                new_rows.append(new_row)

        except Exception as e:
            logger.error(f"Error creating expanded rows: {e}")

        return new_rows

    def _find_anchor_index(self, order_row: pd.Series) -> Optional[int]:
        """Find the anchor index in option_df for the given order row."""
        anchor_candidates = self.option_df[
            (self.option_df["상품명구분1"] == order_row["상품명구분"])
            & (self.option_df["옵션분리구분2"] == order_row["옵션분리"])
        ]

        if anchor_candidates.empty:
            logger.warning(
                f"Could not find anchor row for 상품명구분: {order_row['상품명구분']}"
            )
            return None

        return anchor_candidates.index[0]

    def _create_new_row(self, order_row: pd.Series, option_data_idx: int) -> Dict:
        """Create a new row based on order row and option data."""
        option_source_row = self.option_df.iloc[option_data_idx]

        new_row = {
            "판매몰상품번호/딜번호[출력]": option_source_row["판매몰상품번호/딜번호"],
            "원상품명(쇼핑몰)[출력]": option_source_row["원상품명_쇼핑몰"],
            "원옵션(쇼핑몰)[출력]": (
                option_source_row["원옵션_쇼핑몰"]
                if pd.notna(option_source_row["원옵션_쇼핑몰"])
                else "NO"
            ),
            "수량[출력]": order_row["수량[출력]"],
            "옵션분리": order_row["옵션분리"],
        }

        # Create product identifier for new row
        new_row["상품명구분"] = (
            str(new_row["판매몰상품번호/딜번호[출력]"])
            + str(new_row["원상품명(쇼핑몰)[출력]"])
            + str(new_row["원옵션(쇼핑몰)[출력]"])
        ).replace(" ", "")

        return new_row

    def _combine_rows(self, new_rows_list: List[Dict]) -> None:
        """Combine original order data with new expanded rows."""
        if new_rows_list:
            appended_df = pd.DataFrame(new_rows_list)

            # Ensure same columns
            for col in self.order_df.columns:
                if col not in appended_df.columns:
                    appended_df[col] = None

            appended_df = appended_df[self.order_df.columns]
            self.final_order_df = pd.concat(
                [self.order_df, appended_df], ignore_index=True
            )
        else:
            self.final_order_df = self.order_df.copy()

        # Sort the final dataframe
        self._sort_final_dataframe()

    def _sort_final_dataframe(self) -> None:
        """Sort the final dataframe to keep related items together."""

        def get_base_id(prod_id):
            if isinstance(prod_id, str) and "-" in prod_id:
                return prod_id.split("-")[0]
            return prod_id

        def get_sub_id(prod_id):
            if isinstance(prod_id, str) and "-" in prod_id:
                try:
                    return int(prod_id.split("-")[1])
                except (ValueError, IndexError):
                    return 0
            return 0

        self.final_order_df["_sort_key"] = self.final_order_df[
            "판매몰상품번호/딜번호[출력]"
        ].apply(get_base_id)
        self.final_order_df["_sub_sort_key"] = self.final_order_df[
            "판매몰상품번호/딜번호[출력]"
        ].apply(get_sub_id)

        self.final_order_df = (
            self.final_order_df.sort_values(by=["_sort_key", "_sub_sort_key"])
            .drop(columns=["_sort_key", "_sub_sort_key"])
            .reset_index(drop=True)
        )

    def merge_master_data(self) -> None:
        """Merge master data with the processed order data."""
        if self.final_order_df is None or self.master_df is None:
            raise ValueError("Data not processed. Call previous methods first.")

        logger.info("Merging master data...")

        # Define columns to merge from master data
        master_columns = [
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

        # Create lookup dataframe
        master_lookup_df = self.master_df.drop_duplicates(
            subset=["상품명구분1"], keep="first"
        )[master_columns]

        # Perform left merge
        self.final_order_df = pd.merge(
            self.final_order_df,
            master_lookup_df,
            left_on="상품명구분",
            right_on="상품명구분1",
            how="left",
        )

        # Clean up duplicate column
        if "상품명구분1" in self.final_order_df.columns:
            self.final_order_df = self.final_order_df.drop(columns=["상품명구분1"])

        logger.info("Master data merge completed")

    def calculate_final_values(self) -> None:
        """Calculate final order quantities and totals."""
        if self.final_order_df is None:
            raise ValueError("Data not processed. Call previous methods first.")

        logger.info("Calculating final values...")

        # Calculate order quantity
        self.final_order_df["발주수량"] = (
            self.final_order_df["단위수량"] * self.final_order_df["수량[출력]"]
        )

        # Remove unit quantity column as it's no longer needed
        self.final_order_df.drop(columns="단위수량", inplace=True)

        # Calculate totals
        self.final_order_df["기준판매가합계"] = (
            self.final_order_df["발주수량"] * self.final_order_df["기준판매가"]
        )
        self.final_order_df["매입가합계"] = (
            self.final_order_df["발주수량"] * self.final_order_df["매입단가"]
        )

        logger.info("Final calculations completed")

    def process_all(self) -> pd.DataFrame:
        """
        Execute the complete order processing pipeline.

        Returns:
            pd.DataFrame: The processed order dataframe
        """
        logger.info("Starting complete order processing pipeline...")

        self.load_data()
        self.clean_order_data()
        self.process_option_separation()
        self.expand_option_rows()
        self.merge_master_data()
        self.calculate_final_values()

        logger.info("Order processing pipeline completed successfully")
        return self.final_order_df

    def save_to_csv(self, filename: str = "final_order_df.csv") -> None:
        """
        Save the processed data to CSV file.

        Args:
            filename: Output CSV filename
        """
        if self.final_order_df is None:
            raise ValueError("No processed data to save. Call process_all() first.")

        self.final_order_df.to_csv(filename, index=False)
        logger.info(f"Data saved to {filename}")


def main():
    """Main function to demonstrate usage."""
    try:
        # Initialize processor
        processor = OrderProcessor()

        # Process all data
        result_df = processor.process_all()

        # Save results
        processor.save_to_csv()

        # Print summary
        print(f"\nProcessing completed successfully!")
        print(f"Total rows processed: {len(result_df)}")

    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        raise


if __name__ == "__main__":
    main()
