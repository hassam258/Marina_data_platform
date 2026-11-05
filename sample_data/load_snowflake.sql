/* =============================================================================
   Marina platform - load the sample source data.

   Runs top to bottom in one Snowsight worksheet. Select all, Ctrl+Shift+Enter.

   Everything lands as VARCHAR on purpose. Typed columns would reject
   '$1,200.00' or silently coerce it, and the landing layer's whole premise is
   that it stores what ARRIVED. Cleaning happens downstream, in staging.
   ============================================================================= */


-- -----------------------------------------------------------------------------
-- Databases and schema
-- -----------------------------------------------------------------------------

create database if not exists RAW;
create schema if not exists RAW.marina_raw;
create database if not exists MARINA_DEV;   -- where dbt builds; your profile points here

use schema RAW.marina_raw;


-- -----------------------------------------------------------------------------
-- Source A - marina PMS
-- _loaded_at is the ingestion watermark a real loader would stamp. Declared in
-- the CREATE (Snowflake rejects current_timestamp() as a default on ALTER).
-- -----------------------------------------------------------------------------

create or replace table raw_marina_pms (
    booking_id          varchar,
    customer_full_name  varchar,
    raw_contact         varchar,
    boat_name           varchar,
    hull_id             varchar,
    slip_fee            varchar,
    marina_code         varchar,
    _loaded_at          timestamp_ntz default current_timestamp()
);

insert into raw_marina_pms
    (booking_id, customer_full_name, raw_contact, boat_name, hull_id, slip_fee, marina_code)
values
    ('BK-901', 'John Doe',     '+1 (555) 019-2834', 'Sea Breeze',    'FL-8812 AB',  '$1,200.00', 'MIA-01'),
    ('BK-902', 'J. Doe',       'jdoe_99@gmail.com', 'Seabreeze',     'FL-8812 AB',  '1200',      'MIA-01'),
    ('BK-903', 'Sarah Connor', 'sarah.c@sky.net',   'Terminator II', 'CA 9910-X C', '$2,500.00', 'SAN-02');


-- -----------------------------------------------------------------------------
-- Source B - vendor work orders
-- -----------------------------------------------------------------------------

create or replace table raw_vendor_work_orders (
    work_order_id       varchar,
    vendor_name         varchar,
    raw_service_type    varchar,
    hours_spent         varchar,
    status              varchar,
    created_at          varchar,
    completed_at        varchar,
    _loaded_at          timestamp_ntz default current_timestamp()
);

insert into raw_vendor_work_orders
    (work_order_id, vendor_name, raw_service_type, hours_spent, status, created_at, completed_at)
values
    ('WO-101', 'Marine Tech LLC', 'Engine_Overhaul_Emergency', '6.5', 'Completed',   '2026-03-01 09:00:00', '2026-03-01 15:30:00'),
    ('WO-102', 'Capt. Bob',       'Hull Cleaning & Paint',     '3.0', 'Completed',   '2026-03-02 10:00:00', '2026-03-02 13:00:00'),
    ('WO-103', 'FastDock Inc',    'mech_maint_101',            '4.0', 'In Progress', '2026-03-03 08:00:00', null);


-- -----------------------------------------------------------------------------
-- Source C - billing events
-- -----------------------------------------------------------------------------

create or replace table raw_stripe_subscriptions (
    sub_id              varchar,
    customer_email      varchar,
    plan_name           varchar,
    mrr_amount          varchar,
    event_type          varchar,
    event_timestamp     varchar,
    _loaded_at          timestamp_ntz default current_timestamp()
);

insert into raw_stripe_subscriptions
    (sub_id, customer_email, plan_name, mrr_amount, event_type, event_timestamp)
values
    ('SUB-001', 'jdoe_99@gmail.com', 'Annual Premium Slip', '1200.00', 'new', '2026-01-01 00:00:00'),
    ('SUB-002', 'sarah.c@sky.net',   'Monthly Dockage',     '2500.00', 'new', '2026-02-01 00:00:00');


-- -----------------------------------------------------------------------------
-- Verify. The load is correct only if all of these hold.
-- -----------------------------------------------------------------------------

-- Expect 3 / 3 / 2
select 'raw_marina_pms' as table_name, count(*) as row_count from raw_marina_pms
union all select 'raw_vendor_work_orders', count(*) from raw_vendor_work_orders
union all select 'raw_stripe_subscriptions', count(*) from raw_stripe_subscriptions;

-- BK-901 slip_fee still reads '$1,200.00'; raw_contact holds a phone on BK-901
-- and an email on BK-902 (the column carries either, unlabelled)
select booking_id, raw_contact, slip_fee, hull_id, _loaded_at
from raw_marina_pms
order by booking_id;

-- WO-103 completed_at must be NULL, not an empty string
select work_order_id, status, completed_at, (completed_at is null) as completed_at_is_null
from raw_vendor_work_orders
order by work_order_id;
