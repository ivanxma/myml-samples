DROP TABLE IF EXISTS myinventory_train;
CREATE TABLE IF NOT EXISTS myinventory_train(
   sku               INT  NOT NULL PRIMARY KEY 
  ,national_inv      INT  NOT NULL
  ,lead_time         INT 
  ,in_transit_qty    INT  NOT NULL
  ,forecast_3_month  INT  NOT NULL
  ,forecast_6_month  INT  NOT NULL
  ,forecast_9_month  INT  NOT NULL
  ,sales_1_month     INT  NOT NULL
  ,sales_3_month     INT  NOT NULL
  ,sales_6_month     INT  NOT NULL
  ,sales_9_month     INT  NOT NULL
  ,min_bank          INT  NOT NULL
  ,potential_issue   VARCHAR(3) NOT NULL
  ,pieces_past_due   INT  NOT NULL
  ,perf_6_month_avg  INT  NOT NULL
  ,perf_12_month_avg INT  NOT NULL
  ,local_bo_qty      INT  NOT NULL
  ,deck_risk         VARCHAR(3) NOT NULL
  ,oe_constraint     VARCHAR(3) NOT NULL
  ,ppap_risk         VARCHAR(3) NOT NULL
  ,stop_auto_buy     VARCHAR(3) NOT NULL
  ,rev_stop          VARCHAR(3) NOT NULL
  ,went_on_backorder VARCHAR(3) NOT NULL
);
