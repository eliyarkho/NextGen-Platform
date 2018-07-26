import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from googleads import adwords
import datetime
import time
import os
import boto3
import logging
import logging.config

ACCOUNT = 'pq86148.us-east-1'

USER = 'Eli'

PASSWORD = '125Ashford'

WH = "COMPUTE_WH"

DB = 'MNG'

SCHEMA = 'PUBLIC'

YML_FILE_PATH = 'C:/Users/ashford/PycharmProjects/adwords/venv/Lib/site-packages/googleads/googleads.yml'

S3_ACCESS_KEY = 'AKIAJ7BV64VNN4DXKR5A'

S3_SECRET_KEY = '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'

S3_BUCKET = 'snowflake-data-lake'

LOCAL_ADWORDS_DIR = 'C:/Users/ashford/Documents/design/adwords/api_reports/'


def db_connect(logger):
    try:
        engine = create_engine(URL(
            account=ACCOUNT,
            user=USER,
            password=PASSWORD,
            warehouse=WH,
            database=DB,
            shcema=SCHEMA, ))

    except Exception as e:
        logger.critical("{}".format(e), exc_info=True)

    return engine


def get_snowflake_columns(table, engine):
    df_columns = pd.read_sql_query("show  columns  in table ODS.ADWORDS.{}".format(table), engine)
    columns_list = df_columns['column_name'].tolist()
    columns_list.pop()
    return columns_list


def define_select_clause(snow_flake_columns, client, table, logger):
    adwords_columns = {}
    selection_columns = []

    # Get report fields.
    try:
        report_definition_service = client.GetService(
            'ReportDefinitionService', version='v201806')

        fields = report_definition_service.getReportFields(table)

    except Exception as e:
        logger.error("{}".format(e), exc_info=True)
        return

    for field in fields:
        adwords_columns.update({field['xmlAttributeName'].upper(): field['fieldName']})

    # create a select clause for AWQL
    for column in snow_flake_columns:
        selection_columns.append(adwords_columns.get(column))

    select_clause = ', '.join(selection_columns)

    return select_clause


def get_adwords_accounts(engine, logger):
    account_list = []

    cnx = engine.connect()

    try:
        results = cnx.execute("""
                                 SELECT ACCOUNT_ID
                                 FROM MNG.PUBLIC.ADWORDS_ACCOUNTS;
                               """
                              )
    except Exception as e:
        logger.critical("{}".format(e), exc_info=True)
        return

    for rec in results:
        account_list.append(rec[0])

    return account_list


def csv_files_generator(client, s3_resource, logger, **kwargs):
    # Initialize appropriate service.
    report_downloader = client.GetReportDownloader(version='v201806')

    for account in kwargs.get("accounts"):
        # Create report query.
        report_query = ('''
        SELECT {c}
        FROM {t}
        {w}
        DURING {o}'''.format(c=kwargs.get("select_clause"),
                             t=kwargs.get("table"),
                             w=kwargs.get("where_clause"),
                             o=kwargs.get("offset")))

        # Write query result to output file
        if not os.path.exists(LOCAL_ADWORDS_DIR):
            os.makedirs(LOCAL_ADWORDS_DIR)

        st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d__%H_%M_%S')

        file_name = 'adwords_api_{t}_{a}_{d}.csv'.format(
            a=account,
            t=kwargs.get("table"),
            d=st)

        full_path = LOCAL_ADWORDS_DIR + file_name

        file = open(full_path, 'wb')

        try:
            report_downloader.DownloadReportWithAwql(
                report_query,
                'GZIPPED_CSV',
                file,
                client_customer_id=account,
                skip_report_header=True,
                skip_column_header=False,
                skip_report_summary=True,
                include_zero_impressions=False)

            file.close()

        except Exception as e:
            logger.error("{}".format(e), exc_info=True)
            continue

        move_file_to_s3(s3_resource, file_name, kwargs.get("table"), logger)


def get_adwords_reports(engine, logger):
    report_list = []

    cnx = engine.connect()

    try:
        results = cnx.execute("""
                                  SELECT REPORT_NAME ,where_clause, Loading_data_offset 
                                  FROM MNG.PUBLIC.ADWORDS_REPORTS;
                                """
                              )
    except Exception as e:
        logger.critical("{}".format(e), exc_info=True)
        return

    for rec in results:
        report_list.append(rec)

    return report_list


def data_extraction(engine, client, s3_resource, logger):
    accounts = get_adwords_accounts(engine, logger)

    if len(accounts) == 0:
        return

    report_list = get_adwords_reports(engine, logger)

    if len(accounts) == 0:
        return

    for table, where_clause, offset in report_list:
        sf_columns = get_snowflake_columns(table, engine)
        selection_clause = define_select_clause(sf_columns, client, table, logger)
        if not selection_clause:
            continue
        csv_files_generator(client, s3_resource, logger,
                            select_clause=selection_clause,
                            table=table,
                            offset=offset,
                            accounts=accounts,
                            where_clause=where_clause)


def move_file_to_s3(s3, file_name, report, logger):
    try:
        s3_path = '/'.join(['adwords', report, file_name])
        s3.meta.client.upload_file(LOCAL_ADWORDS_DIR + file_name, S3_BUCKET, Key=s3_path)
    except Exception as e:
        logger.critical("{}".format(e), exc_info=True)


def main():
    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger('adwords_Extractor')

    try:
        client = adwords.AdWordsClient.LoadFromStorage(YML_FILE_PATH)

        s3_resource = boto3.resource = boto3.resource(
                                                        's3',
                                                        aws_access_key_id=S3_ACCESS_KEY,
                                                        aws_secret_access_key=S3_SECRET_KEY,
                                                      )

    except Exception as e:
        logger.critical("{}".format(e), exc_info=True)
        return

    finally:

        engine = db_connect(logger)

        if not engine:
            return

        data_extraction(engine, client, s3_resource, logger)


if __name__ == '__main__':
    main()




