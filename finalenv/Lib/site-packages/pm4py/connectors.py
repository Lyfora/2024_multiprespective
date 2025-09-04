'''
    PM4Py – A Process Mining Library for Python
Copyright (C) 2024 Process Intelligence Solutions UG (haftungsbeschränkt)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see this software project's root or
visit <https://www.gnu.org/licenses/>.

Website: https://processintelligence.solutions
Contact: info@processintelligence.solutions
'''

from typing import Optional
import pandas as pd
from pm4py.objects.ocel.obj import OCEL


def extract_log_outlook_mails() -> pd.DataFrame:
    """
    Extracts the history of conversations from the local instance of Microsoft Outlook
    running on the current computer.

    Columns:
    - **CASE ID (case:concept:name)**: Identifier of the conversation.
    - **ACTIVITY (concept:name)**: Activity performed in the current item (e.g., send e-mail, receive e-mail, refuse meeting).
    - **TIMESTAMP (time:timestamp)**: Timestamp of creation of the item in Outlook.
    - **RESOURCE (org:resource)**: Sender of the current item.

    See also:
    * [MailItem Properties](https://learn.microsoft.com/en-us/dotnet/api/microsoft.office.interop.outlook.mailitem?redirectedfrom=MSDN&view=outlook-pia#properties_)
    * [OlObjectClass Enumeration](https://learn.microsoft.com/en-us/dotnet/api/microsoft.office.interop.outlook.olobjectclass?view=outlook-pia)

    :rtype: ``pd.DataFrame``

    .. code-block:: python3
        import pm4py

        dataframe = pm4py.connectors.extract_log_outlook_mails()
    """
    from pm4py.algo.connectors.variants import outlook_mail_extractor

    return outlook_mail_extractor.apply()


def extract_log_outlook_calendar(
    email_user: Optional[str] = None, calendar_id: int = 9
) -> pd.DataFrame:
    """
    Extracts the history of calendar events (creation, update, start, end)
    into a Pandas DataFrame from the local Outlook instance running on the current computer.

    Columns:
    - **CASE ID (case:concept:name)**: Identifier of the meeting.
    - **ACTIVITY (concept:name)**: One of the following activities: Meeting Created, Last Change of Meeting, Meeting Started, Meeting Completed.
    - **TIMESTAMP (time:timestamp)**: Timestamp of the event.
    - **case:subject**: Subject of the meeting.

    :param email_user: (optional) E-mail address from which the (shared) calendar should be extracted.
    :param calendar_id: Identifier of the calendar for the given user (default: 9).

    :rtype: ``pd.DataFrame``

    .. code-block:: python3
        import pm4py

        # Extract using default parameters
        dataframe = pm4py.connectors.extract_log_outlook_calendar()

        # Extract using a specific email user
        dataframe = pm4py.connectors.extract_log_outlook_calendar("vacation-calendar@workplace.eu")
    """
    from pm4py.algo.connectors.variants import outlook_calendar

    parameters = {}
    parameters[outlook_calendar.Parameters.EMAIL_USER] = email_user
    parameters[outlook_calendar.Parameters.CALENDAR_ID] = calendar_id
    return outlook_calendar.apply(parameters=parameters)


def extract_log_windows_events() -> pd.DataFrame:
    """
    Extracts a process mining DataFrame from all events recorded in the Windows registry.

    Columns:
    - **CASE ID (case:concept:name)**: Name of the computer emitting the events.
    - **ACTIVITY (concept:name)**: Concatenation of the source name of the event and the event identifier.
      (See [Win32_NTLogEvent](https://learn.microsoft.com/en-us/previous-versions/windows/desktop/eventlogprov/win32-ntlogevent))
    - **TIMESTAMP (time:timestamp)**: Timestamp of event generation.
    - **RESOURCE (org:resource)**: Username involved in the event.

    :rtype: ``pd.DataFrame``

    .. code-block:: python3
        import pm4py

        dataframe = pm4py.connectors.extract_log_windows_events()
    """
    from pm4py.algo.connectors.variants import windows_events

    return windows_events.apply()


def extract_log_chrome_history(
    history_db_path: Optional[str] = None,
) -> pd.DataFrame:
    """
    Extracts a DataFrame containing the navigation history of Google Chrome.
    Please ensure that Google Chrome history is closed when extracting.

    Columns:
    - **CASE ID (case:concept:name)**: Identifier of the extracted profile.
    - **ACTIVITY (concept:name)**: Complete path of the website, excluding GET arguments.
    - **TIMESTAMP (time:timestamp)**: Timestamp of the visit.

    :param history_db_path: Path to the Google Chrome history database (default: location of the Windows folder).
    :rtype: ``pd.DataFrame``

    .. code-block:: python3
        import pm4py

        dataframe = pm4py.connectors.extract_log_chrome_history()
    """
    from pm4py.algo.connectors.variants import chrome_history

    parameters = {}
    if history_db_path is not None:
        parameters[chrome_history.Parameters.HISTORY_DB_PATH] = history_db_path
    return chrome_history.apply(parameters=parameters)


def extract_log_firefox_history(
    history_db_path: Optional[str] = None,
) -> pd.DataFrame:
    """
    Extracts a DataFrame containing the navigation history of Mozilla Firefox.
    Please ensure that Mozilla Firefox history is closed when extracting.

    Columns:
    - **CASE ID (case:concept:name)**: Identifier of the extracted profile.
    - **ACTIVITY (concept:name)**: Complete path of the website, excluding GET arguments.
    - **TIMESTAMP (time:timestamp)**: Timestamp of the visit.

    :param history_db_path: Path to the Mozilla Firefox history database (default: location of the Windows folder).
    :rtype: ``pd.DataFrame``

    .. code-block:: python3
        import pm4py

        dataframe = pm4py.connectors.extract_log_firefox_history()
    """
    from pm4py.algo.connectors.variants import firefox_history

    parameters = {}
    if history_db_path is not None:
        parameters[firefox_history.Parameters.HISTORY_DB_PATH] = (
            history_db_path
        )
    return firefox_history.apply(parameters=parameters)


def extract_log_github(
    owner: str = "pm4py",
    repo: str = "pm4py-core",
    auth_token: Optional[str] = None,
) -> pd.DataFrame:
    """
    Extracts a DataFrame containing the history of issues from a GitHub repository.
    Due to API rate limits for public and registered users, only a subset of events may be returned.

    :param owner: Owner of the repository (e.g., pm4py).
    :param repo: Name of the repository (e.g., pm4py-core).
    :param auth_token: Authorization token.
    :rtype: ``pd.DataFrame``

    .. code-block:: python3
        import pm4py

        dataframe = pm4py.connectors.extract_log_github(owner='pm4py', repo='pm4py-core')
    """
    from pm4py.algo.connectors.variants import github_repo

    parameters = {}
    parameters[github_repo.Parameters.OWNER] = owner
    parameters[github_repo.Parameters.REPOSITORY] = repo
    parameters[github_repo.Parameters.AUTH_TOKEN] = auth_token
    return github_repo.apply(parameters)


def extract_log_camunda_workflow(connection_string: str) -> pd.DataFrame:
    """
    Extracts a DataFrame from the Camunda workflow system. In addition to traditional columns,
    the process ID of the process in Camunda is included.

    :param connection_string: ODBC connection string to the Camunda database.
    :rtype: ``pd.DataFrame``

    .. code-block:: python3
        import pm4py

        dataframe = pm4py.connectors.extract_log_camunda_workflow(
            'Driver={PostgreSQL Unicode(x64)};SERVER=127.0.0.3;DATABASE=process-engine;UID=xx;PWD=yy'
        )
    """
    from pm4py.algo.connectors.variants import camunda_workflow

    parameters = {}
    parameters[camunda_workflow.Parameters.CONNECTION_STRING] = (
        connection_string
    )
    return camunda_workflow.apply(None, parameters=parameters)


def extract_log_sap_o2c(
    connection_string: str, prefix: str = ""
) -> pd.DataFrame:
    """
    Extracts a DataFrame for the SAP Order-to-Cash (O2C) process.

    :param connection_string: ODBC connection string to the SAP database.
    :param prefix: Prefix for the tables (e.g., SAPSR3.).
    :rtype: ``pd.DataFrame``

    .. code-block:: python3
        import pm4py

        dataframe = pm4py.connectors.extract_log_sap_o2c(
            'Driver={Oracle in instantclient_21_6};DBQ=127.0.0.3:1521/ZIB;UID=xx;PWD=yy'
        )
    """
    from pm4py.algo.connectors.variants import sap_o2c

    parameters = {}
    parameters[sap_o2c.Parameters.CONNECTION_STRING] = connection_string
    parameters[sap_o2c.Parameters.PREFIX] = prefix
    return sap_o2c.apply(None, parameters=parameters)


def extract_log_sap_accounting(
    connection_string: str, prefix: str = ""
) -> pd.DataFrame:
    """
    Extracts a DataFrame for the SAP Accounting process.

    :param connection_string: ODBC connection string to the SAP database.
    :param prefix: Prefix for the tables (e.g., SAPSR3.).
    :rtype: ``pd.DataFrame``

    .. code-block:: python3
        import pm4py

        dataframe = pm4py.connectors.extract_log_sap_accounting(
            'Driver={Oracle in instantclient_21_6};DBQ=127.0.0.3:1521/ZIB;UID=xx;PWD=yy'
        )
    """
    from pm4py.algo.connectors.variants import sap_accounting

    parameters = {}
    parameters[sap_accounting.Parameters.CONNECTION_STRING] = connection_string
    parameters[sap_accounting.Parameters.PREFIX] = prefix
    return sap_accounting.apply(None, parameters=parameters)


def extract_ocel_outlook_mails() -> OCEL:
    """
    Extracts the history of conversations from the local instance of Microsoft Outlook
    running on the current computer as an object-centric event log.

    Columns:
    - **ACTIVITY (ocel:activity)**: Activity performed in the current item (e.g., send e-mail, receive e-mail, refuse meeting).
    - **TIMESTAMP (ocel:timestamp)**: Timestamp of creation of the item in Outlook.

    Object Types:
    - **org:resource**: Sender of the mail.
    - **recipients**: List of recipients of the mail.
    - **topic**: Topic of the discussion.

    See also:
    * [MailItem Properties](https://learn.microsoft.com/en-us/dotnet/api/microsoft.office.interop.outlook.mailitem?redirectedfrom=MSDN&view=outlook-pia#properties_)
    * [OlObjectClass Enumeration](https://learn.microsoft.com/en-us/dotnet/api/microsoft.office.interop.outlook.olobjectclass?view=outlook-pia)

    :rtype: ``OCEL``

    .. code-block:: python3
        import pm4py

        ocel = pm4py.connectors.extract_ocel_outlook_mails()
    """
    import pm4py

    dataframe = pm4py.connectors.extract_log_outlook_mails()
    return pm4py.convert_log_to_ocel(
        dataframe,
        object_types=["case:concept:name"],
        timestamp_column="time:timestamp",
        additional_object_attributes={"case:concept:name": ["org:resource", "recipients", "topic"]},
    )


def extract_ocel_outlook_calendar(
    email_user: Optional[str] = None, calendar_id: int = 9
) -> OCEL:
    """
    Extracts the history of calendar events (creation, update, start, end)
    as an object-centric event log from the local Outlook instance running on the current computer.

    Columns:
    - **ACTIVITY (ocel:activity)**: One of the following activities: Meeting Created, Last Change of Meeting, Meeting Started, Meeting Completed.
    - **TIMESTAMP (ocel:timestamp)**: Timestamp of the event.

    Object Types:
    - **case:concept:name**: Identifier of the meeting.
    - **case:subject**: Subject of the meeting.

    :param email_user: (optional) E-mail address from which the (shared) calendar should be extracted.
    :param calendar_id: Identifier of the calendar for the given user (default: 9).
    :rtype: ``OCEL``

    .. code-block:: python3
        import pm4py

        # Extract using default parameters
        ocel = pm4py.connectors.extract_ocel_outlook_calendar()

        # Extract using a specific email user
        ocel = pm4py.connectors.extract_ocel_outlook_calendar("vacation-calendar@workplace.eu")
    """
    import pm4py

    dataframe = pm4py.connectors.extract_log_outlook_calendar(
        email_user, calendar_id
    )
    return pm4py.convert_log_to_ocel(
        dataframe,
        object_types=["case:concept:name"],
        timestamp_column="time:timestamp",
        additional_object_attributes={"case:concept:name": ["case:concept:name", "case:subject"]},
    )


def extract_ocel_windows_events() -> OCEL:
    """
    Extracts an object-centric event log from all events recorded in the Windows registry.

    Columns:
    - **ACTIVITY (ocel:activity)**: Concatenation of the source name of the event and the event identifier.
      (See [Win32_NTLogEvent](https://learn.microsoft.com/en-us/previous-versions/windows/desktop/eventlogprov/win32-ntlogevent))
    - **TIMESTAMP (ocel:timestamp)**: Timestamp of event generation.

    Object Types:
    - **categoryString**: Translation of the subcategory. The translation is source-specific.
    - **computerName**: Name of the computer that generated the event.
    - **eventIdentifier**: Identifier of the event, specific to the source that generated the event log entry.
    - **eventType**: Event type classification (1=Error; 2=Warning; 3=Information; 4=Security Audit Success; 5=Security Audit Failure).
    - **sourceName**: Name of the source (application, service, driver, or subsystem) that generated the entry.
    - **user**: Username of the logged-on user when the event occurred. If the username cannot be determined, this will be NULL.

    :rtype: ``OCEL``

    .. code-block:: python3
        import pm4py

        ocel = pm4py.connectors.extract_ocel_windows_events()
    """
    import pm4py

    dataframe = pm4py.connectors.extract_log_windows_events()
    return pm4py.convert_log_to_ocel(
        dataframe,
        object_types="case:concept:name",
        timestamp_column="time:timestamp",
        additional_object_attributes={"case:concept:name": [
            "categoryString",
            "computerName",
            "eventIdentifier",
            "eventType",
            "sourceName",
            "user",
        ]},
    )


def extract_ocel_chrome_history(history_db_path: Optional[str] = None) -> OCEL:
    """
    Extracts an object-centric event log containing the navigation history of Google Chrome.
    Please ensure that Google Chrome history is closed when extracting.

    Columns:
    - **ACTIVITY (ocel:activity)**: Complete path of the website, excluding GET arguments.
    - **TIMESTAMP (ocel:timestamp)**: Timestamp of the visit.

    Object Types:
    - **case:concept:name**: Profile of Chrome used to visit the site.
    - **complete_url**: Complete URL of the website.
    - **url_wo_parameters**: Complete URL excluding the part after '?'.
    - **domain**: Domain of the visited website.

    :param history_db_path: Path to the Google Chrome history database (default: location of the Windows folder).
    :rtype: ``OCEL``

    .. code-block:: python3
        import pm4py

        ocel = pm4py.connectors.extract_ocel_chrome_history()
    """
    import pm4py

    dataframe = pm4py.connectors.extract_log_chrome_history(history_db_path)
    return pm4py.convert_log_to_ocel(
        dataframe,
        object_types=["case:concept:name"],
        timestamp_column="time:timestamp",
        additional_object_attributes={"case:concept:name": [
            "case:concept:name",
            "complete_url",
            "url_wo_parameters",
            "domain",
        ]},
    )


def extract_ocel_firefox_history(
    history_db_path: Optional[str] = None,
) -> OCEL:
    """
    Extracts an object-centric event log containing the navigation history of Mozilla Firefox.
    Please ensure that Mozilla Firefox history is closed when extracting.

    Columns:
    - **ACTIVITY (ocel:activity)**: Complete path of the website, excluding GET arguments.
    - **TIMESTAMP (ocel:timestamp)**: Timestamp of the visit.

    Object Types:
    - **case:concept:name**: Profile of Firefox used to visit the site.
    - **complete_url**: Complete URL of the website.
    - **url_wo_parameters**: Complete URL excluding the part after '?'.
    - **domain**: Domain of the visited website.

    :param history_db_path: Path to the Mozilla Firefox history database (default: location of the Windows folder).
    :rtype: ``OCEL``

    .. code-block:: python3
        import pm4py

        ocel = pm4py.connectors.extract_ocel_firefox_history()
    """
    import pm4py

    dataframe = pm4py.connectors.extract_log_firefox_history(history_db_path)
    return pm4py.convert_log_to_ocel(
        dataframe,
        object_types=["case:concept:name"],
        timestamp_column="time:timestamp",
        additional_object_attributes={"case:concept:name": [
            "case:concept:name",
            "complete_url",
            "url_wo_parameters",
            "domain",
        ]},
    )


def extract_ocel_github(
    owner: str = "pm4py",
    repo: str = "pm4py-core",
    auth_token: Optional[str] = None,
) -> OCEL:
    """
    Extracts an object-centric event log containing the history of issues from a GitHub repository.
    Due to API rate limits for public and registered users, only a subset of events may be returned.

    Columns:
    - **ACTIVITY (ocel:activity)**: The event type (e.g., created, commented, closed, subscribed).
    - **TIMESTAMP (ocel:timestamp)**: Timestamp of the event execution.

    Object Types:
    - **case:concept:name**: URL of the events related to the issue.
    - **org:resource**: Involved resource.
    - **case:repo**: Repository in which the issue was created.

    :param owner: Owner of the repository (e.g., pm4py).
    :param repo: Name of the repository (e.g., pm4py-core).
    :param auth_token: Authorization token.
    :rtype: ``OCEL``

    .. code-block:: python3
        import pm4py

        ocel = pm4py.connectors.extract_ocel_github(owner='pm4py', repo='pm4py-core')
    """
    import pm4py

    dataframe = pm4py.connectors.extract_log_github(owner, repo, auth_token)
    return pm4py.convert_log_to_ocel(
        dataframe,
        object_types=["case:concept:name"],
        timestamp_column="time:timestamp",
        additional_object_attributes={"case:concept:name": ["case:concept:name", "org:resource", "case:repo"]},
    )


def extract_ocel_camunda_workflow(connection_string: str) -> OCEL:
    """
    Extracts an object-centric event log from the Camunda workflow system.

    Columns:
    - **ACTIVITY (ocel:activity)**: Activity performed within Camunda.
    - **TIMESTAMP (ocel:timestamp)**: Timestamp of the activity execution.

    Object Types:
    - **case:concept:name**: Identifier of the case.
    - **processID**: Process ID within Camunda.
    - **org:resource**: Resource involved in the activity.

    :param connection_string: ODBC connection string to the Camunda database.
    :rtype: ``OCEL``

    .. code-block:: python3
        import pm4py

        ocel = pm4py.connectors.extract_ocel_camunda_workflow(
            'Driver={PostgreSQL Unicode(x64)};SERVER=127.0.0.3;DATABASE=process-engine;UID=xx;PWD=yy'
        )
    """
    import pm4py

    dataframe = pm4py.connectors.extract_log_camunda_workflow(
        connection_string
    )
    return pm4py.convert_log_to_ocel(
        dataframe,
        object_types=["case:concept:name"],
        timestamp_column="time:timestamp",
        additional_object_attributes={"case:concept:name": ["case:concept:name", "processID", "org:resource"]},
    )


def extract_ocel_sap_o2c(connection_string: str, prefix: str = "") -> OCEL:
    """
    Extracts an object-centric event log for the SAP Order-to-Cash (O2C) process.

    Columns:
    - **ACTIVITY (ocel:activity)**: Activity performed in the O2C process.
    - **TIMESTAMP (ocel:timestamp)**: Timestamp of the activity execution.

    Object Types:
    - **case:concept:name**: Identifier of the case.
    - **org:resource**: Resource involved in the activity.

    :param connection_string: ODBC connection string to the SAP database.
    :param prefix: Prefix for the tables (e.g., SAPSR3.).
    :rtype: ``OCEL``

    .. code-block:: python3
        import pm4py

        ocel = pm4py.connectors.extract_ocel_sap_o2c(
            'Driver={Oracle in instantclient_21_6};DBQ=127.0.0.3:1521/ZIB;UID=xx;PWD=yy'
        )
    """
    import pm4py

    dataframe = pm4py.connectors.extract_log_sap_o2c(
        connection_string, prefix=prefix
    )
    return pm4py.convert_log_to_ocel(
        dataframe,
        object_types=["case:concept:name"],
        timestamp_column="time:timestamp",
        additional_object_attributes={"case:concept:name": ["case:concept:name", "org:resource"]},
    )


def extract_ocel_sap_accounting(
    connection_string: str, prefix: str = ""
) -> OCEL:
    """
    Extracts an object-centric event log for the SAP Accounting process.

    Columns:
    - **ACTIVITY (ocel:activity)**: Activity performed in the Accounting process.
    - **TIMESTAMP (ocel:timestamp)**: Timestamp of the activity execution.

    Object Types:
    - **case:concept:name**: Identifier of the case.
    - **org:resource**: Resource involved in the activity.

    :param connection_string: ODBC connection string to the SAP database.
    :param prefix: Prefix for the tables (e.g., SAPSR3.).
    :rtype: ``OCEL``

    .. code-block:: python3
        import pm4py

        ocel = pm4py.connectors.extract_ocel_sap_accounting(
            'Driver={Oracle in instantclient_21_6};DBQ=127.0.0.3:1521/ZIB;UID=xx;PWD=yy'
        )
    """
    import pm4py

    dataframe = pm4py.connectors.extract_log_sap_accounting(
        connection_string, prefix=prefix
    )
    return pm4py.convert_log_to_ocel(
        dataframe,
        object_types=["case:concept:name"],
        timestamp_column="time:timestamp",
        additional_object_attributes={"case:concept:name": ["case:concept:name", "org:resource"]},
    )
