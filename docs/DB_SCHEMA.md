-- WARNING: This schema is for context only and is not meant to be run.
-- Table order and constraints may not be valid for execution.

CREATE TABLE public.accounts (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  name text NOT NULL,
  brokerage text NOT NULL,
  old_owner text NOT NULL,
  type text NOT NULL,
  user_id uuid NOT NULL,
  CONSTRAINT accounts_pkey PRIMARY KEY (id),
  CONSTRAINT fk_accounts_user_id FOREIGN KEY (user_id) REFERENCES public.users(id)
);
CREATE TABLE public.asset_price_sources (
  id bigint NOT NULL DEFAULT nextval('asset_price_sources_id_seq'::regclass),
  asset_id bigint NOT NULL,
  source_type text NOT NULL,
  priority integer NOT NULL DEFAULT 1,
  source_params jsonb NOT NULL DEFAULT '{}'::jsonb,
  active boolean NOT NULL DEFAULT true,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT asset_price_sources_pkey PRIMARY KEY (id),
  CONSTRAINT asset_price_sources_asset_id_fkey FOREIGN KEY (asset_id) REFERENCES public.assets(id)
);
CREATE TABLE public.asset_prices (
  price_date date NOT NULL,
  asset_id bigint NOT NULL,
  close_price numeric NOT NULL CHECK (close_price >= 0::numeric),
  currency text NOT NULL,
  source text,
  fetched_at timestamp with time zone,
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT asset_prices_pkey PRIMARY KEY (price_date, asset_id),
  CONSTRAINT fk_asset_prices_asset FOREIGN KEY (asset_id) REFERENCES public.assets(id)
);
CREATE TABLE public.asset_segments (
  asset_id bigint NOT NULL,
  segment_asset_class text NOT NULL,
  weight numeric NOT NULL,
  CONSTRAINT asset_segments_pkey PRIMARY KEY (asset_id, segment_asset_class),
  CONSTRAINT asset_segments_asset_id_fkey FOREIGN KEY (asset_id) REFERENCES public.assets(id)
);
CREATE TABLE public.asset_summary_live (
  asset_id bigint NOT NULL,
  account_id uuid NOT NULL,
  total_quantity numeric NOT NULL,
  total_purchase_amount numeric NOT NULL,
  average_purchase_price numeric NOT NULL,
  current_valuation_price numeric NOT NULL,
  total_valuation_amount numeric NOT NULL,
  unrealized_pnl numeric NOT NULL,
  unrealized_return_rate numeric NOT NULL,
  CONSTRAINT asset_summary_live_pkey PRIMARY KEY (asset_id, account_id),
  CONSTRAINT fk_summary_asset FOREIGN KEY (asset_id) REFERENCES public.assets(id),
  CONSTRAINT fk_summary_account FOREIGN KEY (account_id) REFERENCES public.accounts(id)
);
CREATE TABLE public.assets (
  id bigint NOT NULL DEFAULT nextval('assets_id_seq'::regclass),
  ticker text NOT NULL UNIQUE,
  name_kr text NOT NULL,
  asset_type text NOT NULL,
  currency text NOT NULL,
  market text,
  current_price numeric DEFAULT 0,
  underlying_asset_class text NOT NULL,
  economic_exposure_region text NOT NULL,
  asset_nature text,
  vehicle_type text NOT NULL,
  fx_exposure_type text,
  return_driver text,
  strategy_type text,
  lookthrough_available boolean DEFAULT false,
  price_updated_at timestamp with time zone,
  price_update_status text,
  price_update_error text,
  price_source text,
  CONSTRAINT assets_pkey PRIMARY KEY (id)
);
CREATE TABLE public.codes (
  category text NOT NULL,
  code text NOT NULL,
  display_name_kr text NOT NULL,
  sort_order integer DEFAULT 0,
  CONSTRAINT codes_pkey PRIMARY KEY (category, code)
);
CREATE TABLE public.daily_snapshots (
  date date NOT NULL,
  asset_id bigint NOT NULL,
  account_id uuid NOT NULL,
  quantity numeric NOT NULL,
  valuation_price numeric NOT NULL,
  purchase_price numeric NOT NULL,
  valuation_amount numeric NOT NULL,
  purchase_amount numeric NOT NULL,
  currency text NOT NULL,
  snapshot_price_source text,
  snapshot_generated_at timestamp with time zone,
  CONSTRAINT daily_snapshots_pkey PRIMARY KEY (date, asset_id, account_id),
  CONSTRAINT fk_snapshot_asset FOREIGN KEY (asset_id) REFERENCES public.assets(id),
  CONSTRAINT fk_snapshot_account FOREIGN KEY (account_id) REFERENCES public.accounts(id)
);
CREATE TABLE public.manual_asset_cost_basis_current (
  account_id uuid NOT NULL,
  asset_id bigint NOT NULL,
  currency text NOT NULL,
  cost_basis_amount numeric NOT NULL DEFAULT 0 CHECK (cost_basis_amount >= 0::numeric),
  as_of_date date NOT NULL,
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT manual_asset_cost_basis_current_pkey PRIMARY KEY (account_id, asset_id),
  CONSTRAINT fk_mcb_current_account FOREIGN KEY (account_id) REFERENCES public.accounts(id),
  CONSTRAINT fk_mcb_current_asset FOREIGN KEY (asset_id) REFERENCES public.assets(id)
);
CREATE TABLE public.manual_asset_cost_basis_events (
  id bigint NOT NULL DEFAULT nextval('manual_asset_cost_basis_events_id_seq'::regclass),
  account_id uuid NOT NULL,
  asset_id bigint NOT NULL,
  event_date date NOT NULL,
  delta_amount numeric NOT NULL CHECK (delta_amount <> 0::numeric),
  currency text NOT NULL,
  reason text,
  memo text,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT manual_asset_cost_basis_events_pkey PRIMARY KEY (id),
  CONSTRAINT fk_mcb_event_account FOREIGN KEY (account_id) REFERENCES public.accounts(id),
  CONSTRAINT fk_mcb_event_asset FOREIGN KEY (asset_id) REFERENCES public.assets(id)
);
CREATE TABLE public.recurring_orders (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  account_id uuid NOT NULL,
  asset_id bigint NOT NULL,
  trade_type text NOT NULL DEFAULT 'BUY'::text,
  frequency text NOT NULL CHECK (frequency = ANY (ARRAY['MONTHLY'::text, 'WEEKLY'::text])),
  day_of_month integer,
  day_of_week integer,
  timezone text NOT NULL DEFAULT 'Asia/Seoul'::text,
  quantity numeric,
  price numeric,
  amount numeric,
  currency text,
  start_date date NOT NULL,
  end_date date,
  active boolean NOT NULL DEFAULT true,
  memo text,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT recurring_orders_pkey PRIMARY KEY (id),
  CONSTRAINT fk_recurring_orders_account_id FOREIGN KEY (account_id) REFERENCES public.accounts(id)
);
CREATE TABLE public.transactions (
  id bigint NOT NULL DEFAULT nextval('transactions_id_seq'::regclass),
  transaction_date timestamp with time zone NOT NULL,
  asset_id bigint NOT NULL,
  account_id uuid NOT NULL,
  trade_type text NOT NULL,
  quantity numeric NOT NULL,
  price numeric NOT NULL,
  fee numeric DEFAULT 0,
  tax numeric DEFAULT 0,
  memo text,
  CONSTRAINT transactions_pkey PRIMARY KEY (id),
  CONSTRAINT fk_transaction_asset FOREIGN KEY (asset_id) REFERENCES public.assets(id),
  CONSTRAINT fk_transaction_account FOREIGN KEY (account_id) REFERENCES public.accounts(id)
);
CREATE TABLE public.users (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  username text NOT NULL UNIQUE,
  password text NOT NULL,
  CONSTRAINT users_pkey PRIMARY KEY (id)
);