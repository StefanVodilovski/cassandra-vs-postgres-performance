CREATE TABLE IF NOT EXISTS public.business (
	business_id text NOT NULL,
	"name" text NULL,
	address text NULL,
	city text NULL,
	state text NULL,
	postal_code text NULL,
	latitude float8 NULL,
	longitude float8 NULL,
	stars int4 NULL,
	review_count int4 NULL,
	is_open int4 NULL,
	categories text NULL,
	CONSTRAINT business_pkey PRIMARY KEY (business_id)
);


CREATE TABLE public.review (
	review_id text NOT NULL,
	user_id text NULL,
	business_id text NULL,
	stars int4 NULL,
	useful int4 NULL,
	funny int4 NULL,
	cool int4 NULL,
	"text" text NULL,
	"date" date NULL,
	CONSTRAINT review_pkey PRIMARY KEY (review_id)
);


CREATE TABLE public."user" (
	user_id text NOT NULL,
	"name" text NULL,
	review_count int4 NULL,
	yelping_since date NULL,
	useful int4 NULL,
	funny int4 NULL,
	cool int4 NULL,
	average_stars float8 NULL,
	CONSTRAINT user_pkey PRIMARY KEY (user_id)
);