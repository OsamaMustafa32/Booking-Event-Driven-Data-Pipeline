import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql.functions import upper, col, when, regexp_replace, to_date, to_timestamp
from pyspark.sql.types import DecimalType, IntegerType, LongType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

DATA_QUALITY_THRESHOLD = 0.05
VALID_STATUSES = ["CONFIRMED", "CANCELLED", "MODIFIED", "NO_SHOW"]
QUARANTINE_PATH = "s3://booking-edp-data/quarantine/"

# Read Properties Dim Table from Redshift
PropertiesDimFromRedshift = glueContext.create_dynamic_frame.from_catalog(
    database="booking-edp-catalog",
    table_name="dev_bookings_properties_dim",
    redshift_tmp_dir="s3://booking-edp-data/temporary/properties_dim/",
    transformation_ctx="PropertiesDimFromRedshift",
)

# Read Booking Channels Dim Table from Redshift
ChannelsDimFromRedshift = glueContext.create_dynamic_frame.from_catalog(
    database="booking-edp-catalog",
    table_name="dev_bookings_booking_channels_dim",
    redshift_tmp_dir="s3://booking-edp-data/temporary/channels_dim/",
    transformation_ctx="ChannelsDimFromRedshift",
)

# Read Daily Bookings Data from S3 (via Glue Catalog)
DailyBookingsData = glueContext.create_dynamic_frame.from_catalog(
    database="booking-edp-catalog",
    table_name="booking_edp_data",
    transformation_ctx="DailyBookingsData",
)

# Convert to Spark DataFrames for transformations
bookings_df = DailyBookingsData.toDF()
properties_df = PropertiesDimFromRedshift.toDF()
channels_df = ChannelsDimFromRedshift.toDF()

total_records = bookings_df.count()

# Standardize status column (uppercase)
bookings_df = bookings_df.withColumn(
    "status", upper(regexp_replace(col("status"), "_", " "))
)

bookings_df = bookings_df.withColumn(
    "status",
    when(col("status") == "NO SHOW", "NO_SHOW").otherwise(col("status"))
)

# Cast columns to proper types
bookings_df = bookings_df \
    .withColumn("property_id", col("property_id").cast(LongType())) \
    .withColumn("channel_id", col("channel_id").cast(LongType())) \
    .withColumn("guest_id", col("guest_id").cast(LongType())) \
    .withColumn("rooms", col("rooms").cast(IntegerType())) \
    .withColumn("adults", col("adults").cast(IntegerType())) \
    .withColumn("children", col("children").cast(IntegerType())) \
    .withColumn("amount", col("amount").cast(DecimalType(10, 2))) \
    .withColumn("checkin", to_date(col("checkin"), "yyyy-MM-dd")) \
    .withColumn("checkout", to_date(col("checkout"), "yyyy-MM-dd")) \
    .withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss"))

# Data Quality Checks
valid_property_ids = properties_df.select("property_id").distinct()
valid_channel_ids = channels_df.select("channel_id").distinct()

bad_records = bookings_df.filter(
    (col("booking_id").isNull()) | (col("booking_id") == "") |
    (col("property_id").isNull()) |
    (col("channel_id").isNull()) |
    (col("amount").isNull()) | (col("amount") < 0) |
    (col("checkin").isNull()) | (col("checkout").isNull()) |
    (col("checkout") <= col("checkin")) |
    (~col("status").isin(VALID_STATUSES))
)

good_records = bookings_df.filter(
    (col("booking_id").isNotNull()) & (col("booking_id") != "") &
    (col("property_id").isNotNull()) &
    (col("channel_id").isNotNull()) &
    (col("amount").isNotNull()) & (col("amount") >= 0) &
    (col("checkin").isNotNull()) & (col("checkout").isNotNull()) &
    (col("checkout") > col("checkin")) &
    (col("status").isin(VALID_STATUSES))
)

orphan_properties = good_records.join(
    valid_property_ids, on="property_id", how="left_anti"
)
orphan_channels = good_records.join(
    valid_channel_ids, on="channel_id", how="left_anti"
)

good_records = good_records.join(valid_property_ids, on="property_id", how="inner")
good_records = good_records.join(valid_channel_ids, on="channel_id", how="inner")

all_bad_records = bad_records.unionByName(orphan_properties, allowMissingColumns=True) \
                             .unionByName(orphan_channels, allowMissingColumns=True)

# Write Bad Records to Quarantine

bad_count = all_bad_records.count()
good_count = good_records.count()

if bad_count > 0:
    bad_dynamic_frame = DynamicFrame.fromDF(all_bad_records, glueContext, "bad_dynamic_frame")
    glueContext.write_dynamic_frame.from_options(
        frame=bad_dynamic_frame,
        connection_type="s3",
        connection_options={"path": QUARANTINE_PATH},
        format="csv",
        transformation_ctx="WriteQuarantine",
    )

# Circuit Breaker - Halt if bad records exceed threshold


if total_records > 0:
    bad_ratio = bad_count / total_records
    if bad_ratio > DATA_QUALITY_THRESHOLD:
        raise Exception(
            f"Data quality threshold exceeded: {bad_count}/{total_records} "
            f"({bad_ratio:.2%}) records failed validation. "
            f"Threshold is {DATA_QUALITY_THRESHOLD:.0%}. Pipeline halted."
        )


# Standardize payment_method (snake_case -> Title Case)


good_records = good_records.withColumn(
    "payment_method",
    when(col("payment_method") == "credit_card", "Credit Card")
    .when(col("payment_method") == "debit_card", "Debit Card")
    .when(col("payment_method") == "paypal", "PayPal")
    .when(col("payment_method") == "bank_transfer", "Bank Transfer")
    .when(col("payment_method") == "pay_at_hotel", "Pay at Hotel")
    .when(col("payment_method") == "wallet", "Wallet")
    .otherwise(col("payment_method"))
)

# Join with Properties Dim on property_id
JoinWithProperties = DynamicFrame.fromDF(
    good_records.join(
        properties_df,
        good_records["property_id"] == properties_df["property_id"],
        "left",
    ),
    glueContext,
    "JoinWithProperties",
)

# Select Fields After Properties Join
SelectFieldsAfterProperties = SelectFields.apply(
    frame=JoinWithProperties,
    paths=[
        "booking_id", "guest_id", "channel_id", "status",
        "rooms", "adults", "children", "room_type",
        "checkin", "checkout", "amount", "currency",
        "payment_method", "created_at",
        "property_name", "property_type", "star_rating",
        "chain_name", "city", "country", "country_code",
    ],
    transformation_ctx="SelectFieldsAfterProperties",
)

# Join with Booking Channels Dim on channel_id
SelectFieldsAfterPropertiesDF = SelectFieldsAfterProperties.toDF()
channels_df_for_join = ChannelsDimFromRedshift.toDF()

JoinWithChannels = DynamicFrame.fromDF(
    SelectFieldsAfterPropertiesDF.join(
        channels_df_for_join,
        SelectFieldsAfterPropertiesDF["channel_id"] == channels_df_for_join["channel_id"],
        "left",
    ),
    glueContext,
    "JoinWithChannels",
)

# Select Fields After Channels Join
SelectFieldsAfterChannels = SelectFields.apply(
    frame=JoinWithChannels,
    paths=[
        "booking_id", "guest_id",
        "property_name", "property_type", "star_rating",
        "chain_name", "city", "country", "country_code",
        "channel_name", "channel_type",
        "status", "rooms", "adults", "children", "room_type",
        "checkin", "checkout", "amount", "currency",
        "payment_method", "created_at",
    ],
    transformation_ctx="SelectFieldsAfterChannels",
)

# Apply Final Schema Mapping (rename + type for Redshift)
FinalSchemaMapping = ApplyMapping.apply(
    frame=SelectFieldsAfterChannels,
    mappings=[
        ("booking_id", "string", "booking_id", "varchar"),
        ("guest_id", "long", "guest_id", "bigint"),
        ("property_name", "string", "property_name", "varchar"),
        ("property_type", "string", "property_type", "varchar"),
        ("star_rating", "int", "star_rating", "int"),
        ("chain_name", "string", "chain_name", "varchar"),
        ("city", "string", "city", "varchar"),
        ("country", "string", "country", "varchar"),
        ("country_code", "string", "country_code", "varchar"),
        ("channel_name", "string", "channel_name", "varchar"),
        ("channel_type", "string", "channel_type", "varchar"),
        ("status", "string", "booking_status", "varchar"),
        ("rooms", "int", "num_rooms", "int"),
        ("adults", "int", "num_adults", "int"),
        ("children", "int", "num_children", "int"),
        ("room_type", "string", "room_type", "varchar"),
        ("checkin", "date", "check_in_date", "date"),
        ("checkout", "date", "check_out_date", "date"),
        ("amount", "decimal", "total_amount", "decimal"),
        ("currency", "string", "currency", "varchar"),
        ("payment_method", "string", "payment_method", "varchar"),
        ("created_at", "timestamp", "booking_timestamp", "timestamp"),
    ],
    transformation_ctx="FinalSchemaMapping",
)

# Write Enriched Data to Redshift Fact Table
WriteToRedshift = glueContext.write_dynamic_frame.from_options(
    frame=FinalSchemaMapping,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://booking-edp-data/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "bookings.daily_bookings_fact",
        "connectionName": "redshift-connection",
        "preactions": (
            "CREATE TABLE IF NOT EXISTS bookings.daily_bookings_fact ("
            "booking_id VARCHAR, guest_id BIGINT, "
            "property_name VARCHAR, property_type VARCHAR, star_rating INT, "
            "chain_name VARCHAR, city VARCHAR, country VARCHAR, country_code VARCHAR, "
            "channel_name VARCHAR, channel_type VARCHAR, "
            "booking_status VARCHAR, num_rooms INT, num_adults INT, num_children INT, "
            "room_type VARCHAR, check_in_date DATE, check_out_date DATE, "
            "total_amount DECIMAL(10,2), currency VARCHAR, "
            "payment_method VARCHAR, booking_timestamp TIMESTAMP);"
        ),
    },
    transformation_ctx="WriteToRedshift",
)

job.commit()
