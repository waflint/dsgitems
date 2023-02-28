import os
import pandas as pd
import sqlalchemy as sa
import argparse
import datetime
from io import StringIO
import boto3
from botocore.config import Config

def main():
    import os
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset_date",
        help="dataset date, format YYYY-MM-DD",
        required=True,
        type=datetime.date.fromisoformat)
    args = parser.parse_args()

    # attaching colnames so pandas infers them correctly
    store_sql = (   "SELECT %(dataset_date)s transaction_date, "
                        "'store' channel, "
                        "header.id channel_transaction_id, "
                        "header.customer_id customer_id, "
                        "product.category product_category, "
                        "SUM(detail.discount_price * detail.quantity) discount_total, "
                        "SUM(detail.original_price * detail.quantity) subtotal, "
                        "header.store_number store_no, "
                        "store.division store_division "
                    "FROM dsg.store_transaction_header header "
                    "join dsg.store_transaction_detail detail on header.id = detail.header_id "
                    "join dsg.store_master store on header.store_number = store.store_no "
                    "join dsg.product_master product on detail.sku = product.sku "
                    "where date(header.transaction_date) = %(dataset_date)s "
                    "group by header.header_id, product.category"
    )
    online_sql = (   "SELECT %(dataset_date)s transaction_date, "
                        "'online' channel, "
                        "header.order_id channel_transaction_id, "
                        "header.customer_id customer_id, "
                        "product.category product_category, "
                        "SUM(detail.discount_price * detail.quantity) discount_total, "
                        "SUM(detail.original_price * detail.quantity) subtotal, "
                        "NULL store_no, "
                        "NULL store_division "
                    "FROM dsg.online_order_header header "
                    "join dsg.online_order_detail detail on header.order_id = detail.order_id "
                    "join dsg.product_master product on detail.sku = product.sku "
                    "where date(header.order_date) = %(dataset_date)s "
                    "group by header.order_id, product.category"
    )

    engine = sa.create_engine(f"postgresql://{os.environ['uname']}:{os.environ['pword']}@{os.environ['endpoint']}:5432/postgres")

    # SQLAlchemy does not have ORM level support for partition creation, prepared statements work slightly differently
    # partition must be created before loading corresponding records
    with engine.connect() as conn:
        conn.execute(sa.text("CREATE TABLE partition_:d PARTITION of dsg.omni_transaction FOR VALUES FROM (:d0) TO (:d1);"),
                    {'d': args.dataset_date.strftime('%Y_%m_%d'),'d0':args.dataset_date, 'd1':args.dataset_date + datetime.timedelta(days=1)})
        conn.commit()

    # these could be combined into a single query with a UNION
    df_store = pd.read_sql(sa.text(store_sql), engine.connect(), params={"dataset_date": args.dataset_date})
    df_online = pd.read_sql(sa.text(online_sql), engine.connect(), params={"dataset_date": args.dataset_date})
    df = pd.concat([df_store, df_online],ignore_index=True)

    # Relational load
    df.to_sql(name="omni_transaction", con=engine.connect(), schema="dsg", if_exists="append", index=False, method="multi")


    # datalake export using physical partitioning by date, though to optimize for this use, splitting date into YYYY, MM, and DD components is preferred

    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3',
        aws_access_key_id=os.environ["ACCESS_KEY"],
        aws_secret_access_key=os.environ["SECRET_KEY"],)
    s3_resource.Object('dsg', f'omni_transaction/transaction_date={args.dataset_date}/df.csv').put(Body=csv_buffer.getvalue())

    return

if __name__ == "__main__":
    main()
