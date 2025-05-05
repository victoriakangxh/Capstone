import pandas as pd


def load_df(filepath, datatypes) -> pd.DataFrame:
    """Load data in df
    Args :
        filepath (String) : name of filePath
        datatype (Dict) : data conversion
    Returns :
        df : (pd.DataFrame) : loaded dataFrame.
    """
    df = pd.read_csv(filepath, parse_dates=["datetime"], dtype=datatypes)
    return df


def preprocess(df, *modifications) -> pd.DataFrame:
    """Apply preprocessing helper functions

    Args:
        *modifications (function) | Optional : preprocessing methods to apply

    Returns:
        df (pd.DataFrame) : preprocessed dataframe.
    """
    df = df[~df["Diner or Sicpama member"].isnull()]
    df = get_valid_events(df)
    for proc in modifications:
        df = df.apply(proc, axis=1)
    return df


def get_valid_events(df):
    """Remove all invalid sessions which is not done through scanning

    Args :
        df (pd.DataFrame) : the dataframe to check to

    Returns :
        valid_events (pd.DataFrame) : valid events for analysis
    """

    event_group = df.groupby(
        ["event_name", "user_pseudo_id"]).nunique().reset_index()

    valid_users = event_group[event_group.event_name ==
                              "session_start"].user_pseudo_id
    valid_events = df[df.user_pseudo_id.isin(valid_users)]

    return valid_events


def sort_df(df):
    """Sort the dataframe by user_id and event time

    Args :
        df (pd.DataFrame) : dataframe to sort

    Returns :
        sorted_df (pd.DataFrame) : sorted dataframe.
    """

    sorted_df = df.sort_values(by=["user_pseudo_id", "datetime"]).reset_index()

    return sorted_df


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


def initialize_counter(variables):
    """Initialize an empty counter for the variables

    Args :
        variables (Collection) : the unique collection of key variables

    Returns :
        counter (Dict) : a frequency counter
    """
    counter = {}
    for variable in variables:
        counter[variable] = 0
    return counter


def get_order_process(df):
    """Get the order process for SICPAMA

    This method gets the process counts of each unique process from the events dataframe

    Args :
        df (pd.DataFrame) : the dataframe to process

    Returns :
        funnel_df (pd.DataFrame) : the dataframe containing the execution count of each process

    """

    lastRowIndex = df.shape[0] - 1
    unique_uris = list(df.URI.unique())
    additional_processes = ["session_start", "success", "failed"]
    # Extend the unique_uris list with additional_processes
    processes = unique_uris + additional_processes

    funnel = initialize_counter(processes)
    seen = set()
    for index, row in df.iterrows():
        userid = row.user_pseudo_id  # current user_id
        nextrow = df.iloc[min(index + 1, lastRowIndex)]
        nextuserid = nextrow.user_pseudo_id  # next user_id

        process = row.URI  # use uri as a base process

        if (
            row.event_name == "session_start"
        ):  # if event.name == session start this is a qr scan
            process = row.event_name  # switch uri of menus to scan

        event = (userid, process)  # make a unique event object

        if not event in seen:
            seen.add(event)
            funnel[process] += 1

        if (
            str(process).startswith("payment")
            and (nextuserid != userid)
            or (index == lastRowIndex)
        ):
            funnel[row.sessionStatus.lower()] += 1

    funnel_df = pd.DataFrame(list(funnel.items()),
                             columns=["Process", "Counts"])
    return funnel_df
