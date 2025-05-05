import re
import gc
import datetime
import pandas as pd


def identity(json_string):
    return json_string


# Define a function to process the DataFrame and extract detailed information.
def make_detail(df):
    # Define a nested function to extract URI from a URL.
    def extract_uri(url):
        """Extract URI from scipama domain"""
        # Split the URL by "sicpama.com/" and take the latter part.
        url = str(url).split("sicpama.com/")[-1]
        # Conditional logic to categorize the URL based on specific path components.
        if "callback" in url:
            return "callback_from_sns"
        elif "auth" in url:
            return "auth"
        elif "token" in url:
            return "token"
        elif "customer-receipt?resultCode=0000&status=approvement" in url:
            return "customer-receipt"
        elif "payments/receipt?" in url:
            return "payments/receipt"
        else:
            return url
        # Convert event_timestamp to datetime format.

    df["datetime"] = df["event_timestamp"].map(
        lambda x: datetime.datetime.fromtimestamp(float(x) / 1000000)
    )

    # Normalize event_params JSON Column into separate columns for each key in the JSON objects.
    df_detail = df.merge(
        pd.json_normalize(
            df["event_params"].map(lambda x: {_["key"]: _["value"] for _ in x})
        ),
        left_index=True,
        right_index=True,
    )

    # Simplify column names by removing Parent field indicators.
    df_detail.columns = df_detail.columns.map(lambda x: x.split(".")[0])

    # Drop columns that only contain missing values.
    df_detail = df_detail.dropna(axis=1, how="all")

    # Sort DataFrame by user_pseudo_id and user_id for further processing.
    df_detail = df_detail.sort_values(["user_pseudo_id", "user_id"])
    # Forward and backward fill the user_id within each group of user_pseudo_id.
    df_detail["user_id"] = df_detail.groupby(["user_pseudo_id"])["user_id"].transform(
        lambda x: x.ffill().bfill()
    )

    # Flag visits to the login page based on the presence of 'callback' in the page_location.
    df_detail = df_detail.sort_values(["user_pseudo_id", "datetime"]).reset_index(
        drop=True
    )
    index = df_detail[df_detail["page_location"].str.contains("callback") == True].index

    df_detail.loc[index, "visit_login_page"] = True
    df_detail["visit_login_page"] = df_detail.groupby("user_pseudo_id")[
        "visit_login_page"
    ].transform(lambda x: x.ffill().bfill())

    # Apply the URI extraction function to the page_location column.
    df_detail["URI"] = df_detail["page_location"].map(extract_uri)

    return df_detail


def define_css_path(css_path):
    # Extract the last component after the ">" character and the last id after "div#".
    last = css_path.split(">")[-1]
    last_id = css_path.split("div#")[-1]
    # Categorize CSS paths based on their starting pattern and content.
    # The function checks for specific patterns in the CSS path to assign a human-readable category.
    # This includes checks for authentication buttons, menu buttons, tabs, and other UI elements.
    # Each `elif` block checks for a specific pattern and returns a corresponding label.

    if css_path.startswith("food-courts"):
        return "food-courts"
        # auth
    elif (
        css_path.startswith("auth")
        and "btn flex gap-2 items-center justify-center bg-[#F0F0F0] text-black w-full px-6 py-4 rounded mt-3"
        in css_path
    ):
        return "diner-auth-btn_guest_login"
    elif (
        css_path.startswith("auth")
        and "btn flex gap-2 items-center justify-center bg-[#FEE500] text-black w-full px-6 py-4 rounded mt-3"
        in css_path
    ):
        return "diner-auth-btn_kakao_login"
    elif (
        css_path.startswith("auth")
        and "btn flex gap-2 items-center justify-center bg-[#1877F2] text-white w-full px-6 py-4 rounded mt-3"
        in css_path
    ):
        return "diner-auth-btn_facebook_login"
    elif (
        css_path.startswith("auth")
        and "btn flex gap-2 items-center justify-center bg-white text-black w-full px-6 py-4 rounded mt-3"
        in css_path
    ):
        return "diner-auth-btn_google_login"
    elif (
        css_path.startswith("auth")
        and "w-8 h-8 rounded-full absolute top-4 right-4 flex items-center justify-center"
        in css_path
    ):
        return "diner-auth-btn_close_login_page"
    elif (
        css_path.startswith("auth")
        and "relative h-full w-full flex flex-col items-center p-8 !pb-10" in last
    ):
        return "diner-auth-bottom_sheet_background"
    elif css_path.startswith("auth") and "mantine-Text-root text" in last:
        return "diner-auth-phrase_for_login"
    elif (
        css_path.startswith("auth")
        and "div.absolute z-[1000] bottom-0 left-0 right-0 h-auto bg-white rounded-t"
        in css_path
    ):
        return "diner-auth-login_banner"
    elif (
        css_path.startswith("auth")
        and "absolute z-[100] inset-0 opacity-70 bg-black" in css_path
    ):
        return "diner-auth-banner_above_login_banner"
        # menu
    elif "quick_add_menu_" in last_id:
        return "diner-menus-btn_quick_add_menu"
    elif (
        css_path.startswith("menus")
        and "div.react-modal-sheet-content >div.flex flex-col w-full h-full>div.flex justify-center w-full px-4 mt-auto mb-2 shrink-0"
        in css_path
    ):
        return "diner-menus-btn_put_menu"
    elif "event_menu_" in last_id:
        return "diner-menu-horizontal_scroll_menus"
    elif "category_" in last_id:
        return "diner-menus-btn_category"
    elif "menus" in css_path and "react-modal-sheet-backdrop" in last:
        return "diner-menus-btn_back_from_bottom_sheet"
    elif css_path.startswith("menus") and "div.w-full py-4 px-4" in last:
        return "diner-menus-bg_horizontal_scroll_banner"

    elif "tab" in css_path and "tab_del_menu" in last_id:
        return "diner-tab-btn_del_menu"
    elif (
        "menus" in css_path
        and "w-full flex flex-col justify-center items-center px-4 fixed bottom-4"
        in css_path
    ):  # "mantine-UnstyledButton-root mantine-Button-root text-center bg-theme mantine-12jz2g6"
        return "diner-menus-btn_to_tab"
    elif (
        "menus" in css_path
        and "mantine-Input-wrapper mantine-TextInput-wrapper mantine-1v7s5f8"
        in css_path
    ):
        return "diner-menus-btn_to_search"
    elif (
        "menus" in css_path
        and "mantine-UnstyledButton-root mantine-Button-root text-center bg-theme mantine-59f7h8"
        in last
    ):
        return "diner-menus-btn_put_menu"
    elif "arrow-progress-bar" in css_path:
        return "diner-menus-top_progess_bar"
    elif css_path.startswith("menus") and "img.w-full mt-2 mb-3" in last:
        return "diner-menus-img_of_bottom_sheet"
    elif "menus" in css_path and last_id.startswith("menu_"):
        return "diner-menus-btn_menu_banner"
    elif (
        css_path.startswith("menus")
        and "mantine-UnstyledButton-root mantine-Button-root bg-theme !h-8 !w-fit pl-1 pr-0 mantine-qulz05"
        in css_path
    ):
        return "diner-menus-btn_to_food_court"

    elif (
        css_path.startswith("menus") and "mantine-Badge-root mantine-slrg7n" in css_path
    ):
        return "diner-menus-profile-top-banner"
    elif (
        css_path.startswith("menus")
        and "mantine-Avatar-root mantine-qaascs" in css_path
    ):
        return "diner-menus-profile-menu-banner"
    elif css_path.startswith("menus") and "div.react-modal-sheet-content" in css_path:
        return "diner-menus-others_in_menu_details"
    elif css_path.startswith("menus") and "w-full py-4 px-4" in css_path:
        return "diner-menus-horizontal_menu_banner"
    elif (
        css_path.startswith("menus")
        and "px-4 py-1 overflow-hidden sticky top-[136px] z-10 shrink-0 mt-6 bg-white border-b"
        in css_path
    ):
        return "diner-menus-catergory_banner"
    elif css_path.startswith("menus") and "flex flex-col bg-white p-4 min-h-0 mb-4":
        return "diner-menus-menu_items_banner"
    elif len(re.findall("w-full bg-\[[#a-z0-9A-Z]+\] mt-[46px] py-8 px-6", css_path)):
        return "diner-menus-business_information"
        # elif css_path.startswith('menus')  and "div.w-full bg-[#f7f7f7] mt-[46px] py-8 px-6" in css_path:
        #     return "diner-menus-business_information"
        # Tab
    elif css_path.startswith("tab") and "react-modal-sheet-content" in css_path:
        return "diner-tab-menu_option_bottom_sheet"
    elif (
        css_path.startswith("tab")
        and "mantine-p2utts mantine-Accordion-chevron" in css_path
    ):
        return "diner-tab-btn_open_others_menu_item_toggle"
    elif (
        css_path.startswith("tab")
        and "mantine-Accordion-item mantine-1h0npq1" in css_path
    ):
        return "diner-tab-others_item_banner"
    elif "select_ds_btn" in last_id:
        return "diner-tab-btn_to_menus"
    elif "tab_chg_amt" in last_id:
        return "diner-tab-tab_chg_amt"
    elif (
        css_path.startswith("tab")
        and "mantine-UnstyledButton-root mantine-ActionIcon-root mantine-CloseButton-root mantine-Modal-close mantine-1fholi4"
        in css_path
    ):
        return "diner-tab-close_login_page"
    elif (
        css_path.startswith("tab")
        and "mantine-Paper-root mantine-Modal-content mantine-Modal-content mantine-18wq4n0"
        in css_path
    ):
        return "diner-tab-login-banner"
    elif "go_to_pay_bt" in last_id:
        return "diner-tab-btn_to_payments"
    elif "its_on_me" in last_id:
        return "diner-pay-its_on_me"
    elif css_path.startswith("menus") and "div.react-modal-sheet-header" in css_path:
        return "diner-menus-drag_handle_in_bottom_sheet"
    elif "pay_mine" in last_id:
        return "diner-pay-pay_mine"

    elif "split_evenly" in last_id:
        return "diner-pay-split_evenly"

    elif "spin_wheel" in last_id:
        return "diner-pay-spin_wheel"
    elif css_path.startswith("tab") and "px-4 py-6" in css_path:
        return "diner-tab-my_menu_item_banner"
    elif css_path.startswith("tab") and "flex flex-col w-full h-full p-2" in css_path:
        return "diner-tab-coupon_banner"
    elif (
        css_path.startswith("tab")
        and "flex items-center justify-center gap-4 mt-4" in css_path
    ):
        return "diner-tab-payment_option_banner"
    elif css_path.startswith("tab"):
        return "diner-tab-undefined"
    elif css_path.startswith("stores/"):
        return "diner-stores-all"
    elif css_path.startswith("search"):
        return "diner-search-all"
    elif css_path.startswith("auth"):
        return "diner-auth-all"
    elif css_path.startswith("payment"):
        return "diner-payment-all"
    elif css_path.startswith("spin-the-wheel"):
        return "diner-spin_the_wheel-all"
    elif css_path.startswith("customer-receipt"):
        return "diner-customer_receipt-all"
    elif css_path.startswith("feedback"):
        return "diner-feedback-all"
    elif css_path.startswith("report-error"):
        return "diner-report_error-all"
        # Return the original css_path if no known category matches.
    else:
        return css_path


def transform_stage_1(df):
    # Apply the processing function to the DataFrame.

    df_ga_event_params = make_detail(df)

    # Further processing to filter rows based on specific conditions in the page_location column.
    df_ga_event_params_diner = df_ga_event_params[
        df_ga_event_params["page_location"].str.contains("sg.sicpama|order.sicpama")
    ]
    # # Convert user_pseudo_id to string type.
    df_ga_event_params_diner["user_pseudo_id"] = df_ga_event_params_diner[
        "user_pseudo_id"
    ].astype(str)
    # # Forward and backward fill the myCurrentOrderId within each group of user_pseudo_id and ga_session_id.
    df_ga_event_params_diner[
        "myCurrentOrderId_filled"
    ] = df_ga_event_params_diner.groupby(["user_pseudo_id", "ga_session_id"])[
        "myCurrentOrderId"
    ].transform(lambda x: x.ffill().bfill())
    # df_ga_event_params_diner[
    #     "myCurrentOrderId_filled"
    # ] = df_ga_event_params_diner.groupby(["user_pseudo_id", "ga_session_id"])[
    #     "myCurrentOrderId_filled"
    # ].transform(lambda x: x.ffill().bfill())
    cols=pd.Series(df_ga_event_params_diner.columns)
    for dup in df_ga_event_params_diner.columns[df_ga_event_params_diner.columns.duplicated(keep=False)]: 
        cols[df_ga_event_params_diner.columns.get_loc(dup)] = ([dup + '.' + str(d_idx) 
                                        if d_idx != 0 
                                        else dup 
                                        for d_idx in range(df_ga_event_params_diner.columns.get_loc(dup).sum())]
                                        )
    df_ga_event_params_diner.columns=cols

    return df_ga_event_params_diner


def transform_stage_2(df: pd.DataFrame) :
    """
    Transform the dataframe according to some filters defined below

    """
    # Filter df_ga_event_params_diner for rows where:
    # 1. 'myCurrentOrderId_filled' is not null OR 'user_id' is not null,
    # 2. 'URI' does not start with 'stores',
    # 3. 'event_name' is 'Click'.
    # From these filtered rows, to extract unique 'user_pseudo_id's which's have at least one click in SicPama service.
    orderIds_diner = df[
        (
            (
                df["myCurrentOrderId_filled"].notna()
            )  # Condition 1: 'myCurrentOrderId_filled' is not NA
            | (df["user_id"].notna())
        )  # OR 'user_id' is not NA
        # Condition 2: 'URI' does not start with 'stores'
        & (df["URI"].str.startswith("stores") != True)
        # Condition 3: 'event_name' is 'Click'
        & (df["event_name"] == "Click")
    ][
        "user_pseudo_id"
    ].unique()  # Extract unique 'user_pseudo_id's meeting the above conditions

    # Use the filtered list of 'user_pseudo_id's to select matching rows from df_ga_event_params_diner.
    # .copy() is used to ensure the result is a separate DataFrame, not a view of the original.
    df_orderIds = df[df["user_pseudo_id"].isin(orderIds_diner)].copy()

    # Assuming df_orderIds is a DataFrame that contains a column named 'device',
    # which itself is a dictionary (or similar structure) that includes a 'category' key.

    # The following line extracts the 'category' value from each dictionary in the 'device' column
    # and creates a new column in the DataFrame called 'device.category'.
    df_orderIds["device.category"] = df_orderIds["device"].map(lambda x: x["category"])

    # Filter the DataFrame to include only rows where the 'device.category' is 'mobile'.
    # This effectively selects users who were on mobile devices.
    df_orderIds_m = df_orderIds[df_orderIds["device.category"] == "mobile"]

    return df_orderIds_m


def transform_stage_3(ga_df, order_df, sessions_df):
    df_order_status_joined = order_df.merge(
        sessions_df, how="left", on="orderId"
    ).copy()

    # Merge the above DataFrame with df_orderIds_m (assumed to contain mobile device user data) on the orderId column.
    # This step integrates detailed order and session information with user interaction data from mobile devices.
    df_orderIds_m_joined = ga_df.merge(
        df_order_status_joined,
        how="left",
        left_on="myCurrentOrderId_filled",
        right_on="orderId",
    ).copy()

    # Reset the index of the merged DataFrame for clean, sequential indexing.
    df_orderIds_m_joined = df_orderIds_m_joined.reset_index()

    # Fill missing values in Click_X columns with empty strings for cleaner data representation.
    # It prepares the data for concatenation by ensuring there are no null values.
    df_orderIds_m_joined.loc[
        :, [f"Click_{idx}" for idx in range(5, 0, -1)]
    ] = df_orderIds_m_joined[[f"Click_{idx}" for idx in range(5, 0, -1)]].fillna("")

    # Concatenate values in Click_X columns to form a single CSS_PATH column.
    # This operation aggregates multiple click event data into a single path for easier analysis.
    df_orderIds_m_joined["CSS_PATH"] = df_orderIds_m_joined.apply(
        lambda x: "".join([x[f"Click_{idx}"] for idx in range(5, 0, -1)]), axis=1
    )

    print("Printing df ..... ")
    print(df_orderIds_m_joined.head())
    # Select and rename specific columns for a simplified DataFrame focusing on key data points.
    df_orderIds_simple = (
        df_orderIds_m_joined[
            [
                "datetime",
                "event_name",
                "user_pseudo_id",
                "page_location",
                "page_referrer",
                "engagement_time_msec",
                "orderId",
                "customerId",
                "Status",
                "login",
                "sessionStatus",
                "unique_customerId_count",
                "CSS_PATH",
                "ga_session_number",
                "URI",
                "Diner or Sicpama member",
                "storeId",
            ]
        ]
        .rename({"login": "loginStatus"}, axis="columns")
        .copy()
    )

    # Identify and update the CSS_PATH column for rows with 'Click' event_name by prepending the URI to the existing CSS_PATH.
    # This enhances the CSS_PATH with more specific location data from the URI column.
    index_click_event = df_orderIds_simple[
        df_orderIds_simple["event_name"] == "Click"
    ].index
    df_orderIds_simple.loc[index_click_event, "CSS_PATH"] = (
        df_orderIds_simple.loc[index_click_event, "URI"]
        + "||"
        + df_orderIds_simple.loc[index_click_event, "CSS_PATH"]
    )

    # Merge the simplified DataFrame with a pivot table that aggregates total engagement time per orderId.
    # This step adds a column representing the total engagement time for each order, providing insight into user interaction duration.
    df_orderIds_simple = df_orderIds_simple.merge(
        df_orderIds_simple.pivot_table(
            index="orderId", values="engagement_time_msec", aggfunc="sum"
        )
        .rename({"engagement_time_msec": "engagement_msec_total"}, axis="columns")
        .reset_index(),
        how="left",
        on="orderId",
    )

    # Label CSS paths for "Click" events using the defined function.
    index = df_orderIds_simple[df_orderIds_simple["event_name"] == "Click"].index
    df_orderIds_simple.loc[index, "CSS_PATH_labeled"] = df_orderIds_simple.loc[
        index, "CSS_PATH"
    ].map(define_css_path)

    return df_orderIds_simple


def preprocess(df, *modifications) -> pd.DataFrame:
    """Apply preprocessing helper functions

    Args:
        *modifications (function) | Optional : preprocessing methods to apply

    Returns:
        df (pd.DataFrame) : preprocessed dataframe.
    """
    df = df[~df["Diner or Sicpama member"].isnull()]
    for proc in modifications:
        df = df.apply(proc, axis=1)
    return df


def modify_uri(row):
    """Preprocessing helper : Modifies the URI based on the presence of prefixes in checkers.

    Args :
        row (Series) : the row of the dataframe

    Returns :
        row (Series) : the row after applying the modification
    """
    checkers = ["?", "payment"]
    for checker in checkers:
        if str(row.URI).startswith(checker):
            row.URI = "payment/approval"

    return row


def modify_css(row):
    """Preprocessing helper : Group the CSS_labeled column for callback and token

    Args :
        row (Series) : the row of the dataframe

    Returns :
        row (Series) : the row after applying the modification
    """
    if "callback" in str(row.CSS_PATH_labeled):
        row.CSS_PATH_labeled = "callback"
    elif "token" in str(row.CSS_PATH_labeled):
        row.CSS_PATH_labeled = "token"
    return row


def cleanup(obj):
    """
    Helper to delete variables that are not in used
    free up RAM for other processes

    Args :
        obj : (Any)
    """
    del obj
    gc.collect()
