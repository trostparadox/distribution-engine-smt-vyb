-- Adminer 4.8.0 PostgreSQL 10.15 (Ubuntu 10.15-0ubuntu0.18.04.1) dump

DROP TABLE IF EXISTS "account_history";
CREATE TABLE "public"."account_history" (
    "account" character varying(20) NOT NULL,
    "token" character varying(20) NOT NULL,
    "timestamp" timestamp NOT NULL,
    "quantity" numeric DEFAULT '0' NOT NULL,
    "trx" character varying(50),
    "type" character varying(30) NOT NULL,
    "authorperm" character varying(300)
) WITH (oids = false);

CREATE INDEX "account_history_account_token_timestamp" ON "public"."account_history" USING btree ("account", "token", "timestamp" DESC);

CREATE INDEX "account_history_token_timestamp" ON "public"."account_history" USING btree ("token", "timestamp" DESC);


DROP TABLE IF EXISTS "accounts";
CREATE TABLE "public"."accounts" (
    "name" character varying(20) NOT NULL,
    "symbol" character varying(20) NOT NULL,
    "last_post" timestamp DEFAULT '2019-01-01 00:00:00' NOT NULL,
    "last_root_post" timestamp DEFAULT '2019-01-01 00:00:00' NOT NULL,
    "muted" boolean DEFAULT false NOT NULL,
    "last_follow_refresh_time" timestamp,
    CONSTRAINT "accounts_name_token" PRIMARY KEY ("name", "symbol")
) WITH (oids = false);


DROP TABLE IF EXISTS "configuration";
DROP SEQUENCE IF EXISTS configuration_id_seq;
CREATE SEQUENCE configuration_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;

CREATE TABLE "public"."configuration" (
    "id" integer DEFAULT nextval('configuration_id_seq') NOT NULL,
    "last_streamed_block" integer DEFAULT '0' NOT NULL,
    "last_streamed_timestamp" timestamp,
    "last_engine_streamed_block" integer DEFAULT '0',
    "last_engine_streamed_timestamp" timestamp,
    "name" character varying(20)
) WITH (oids = false);

CREATE INDEX "ix_configuration_87ea5dfc8b8e384d" ON "public"."configuration" USING btree ("id");

INSERT INTO "configuration" ("id", "last_streamed_block", "last_streamed_timestamp", "last_engine_streamed_block", "last_engine_streamed_timestamp", "name") VALUES
(2,	0,	NULL,	0,	NULL,	'ENGINE_SIDECHAIN'),
(1,	0,	NULL,	0,	NULL,	'HIVED');

DROP TABLE IF EXISTS "follows";
CREATE TABLE "public"."follows" (
    "follower" character varying(20) NOT NULL,
    "following" character varying(20) NOT NULL,
    "state" smallint NOT NULL,
    CONSTRAINT "follows_follower_following" PRIMARY KEY ("follower", "following")
) WITH (oids = false);

CREATE INDEX "follows_follower_state" ON "public"."follows" USING btree ("follower", "state");


DROP TABLE IF EXISTS "post_metadata";
CREATE TABLE "public"."post_metadata" (
    "authorperm" character varying(300) NOT NULL,
    "body" text NOT NULL,
    "json_metadata" text NOT NULL,
    "tags" text,
    "children" integer,
    "parent_authorperm" character varying(300),
    CONSTRAINT "post_metadata_authorperm" PRIMARY KEY ("authorperm")
) WITH (oids = false);

CREATE INDEX "post_metadata_parent_authorperm" ON "public"."post_metadata" USING btree ("parent_authorperm");


DROP TABLE IF EXISTS "posts";
CREATE TABLE "public"."posts" (
    "authorperm" character varying(300) NOT NULL,
    "author" character varying(20) NOT NULL,
    "created" timestamp NOT NULL,
    "tags" character varying(256),
    "app" character varying(256),
    "main_post" boolean DEFAULT true NOT NULL,
    "decline_payout" boolean DEFAULT false NOT NULL,
    "token" character varying(30) NOT NULL,
    "vote_rshares" numeric DEFAULT '0' NOT NULL,
    "cashout_time" timestamp,
    "last_payout" timestamp DEFAULT '1970-01-01 00:00:00' NOT NULL,
    "total_payout_value" numeric DEFAULT '0' NOT NULL,
    "curator_payout_value" numeric DEFAULT '0' NOT NULL,
    "score_trend" real DEFAULT '0' NOT NULL,
    "score_hot" real DEFAULT '0' NOT NULL,
    "beneficiaries_payout_value" bigint DEFAULT '0' NOT NULL,
    "promoted" numeric DEFAULT '0' NOT NULL,
    "title" character varying(512),
    "desc" character varying(512),
    "children" integer,
    "parent_author" character varying(20),
    "parent_permlink" character varying(256),
    "score_promoted" real DEFAULT '0' NOT NULL,
    "muted" boolean DEFAULT false NOT NULL,
    CONSTRAINT "posts_authorperm_token" PRIMARY KEY ("authorperm", "token")
) WITH (oids = false);

CREATE INDEX "posts_token_author_created" ON "public"."posts" USING btree ("token", "author", "created" DESC);

CREATE INDEX "posts_token_cashout" ON "public"."posts" USING btree ("token", "cashout_time" DESC);

CREATE INDEX "posts_token_main_post_created_tags" ON "public"."posts" USING btree ("token", "main_post", "created" DESC, "tags");

CREATE INDEX "posts_token_main_post_promoted" ON "public"."posts" USING btree ("token", "main_post", "promoted" DESC);

CREATE INDEX "posts_token_main_post_score_hot" ON "public"."posts" USING btree ("token", "main_post", "score_hot");

CREATE INDEX "posts_token_main_post_score_trend" ON "public"."posts" USING btree ("token", "main_post" DESC, "score_trend");


DROP TABLE IF EXISTS "reblogs";
CREATE TABLE "public"."reblogs" (
    "account" character varying(20) NOT NULL,
    "authorperm" character varying(300) NOT NULL,
    "timestamp" timestamp NOT NULL
) WITH (oids = false);

CREATE INDEX "ix_reblogs_bd3dc6ff219ad07d" ON "public"."reblogs" USING btree ("account", "authorperm");

CREATE INDEX "reblogs_authorperm_timestamp" ON "public"."reblogs" USING btree ("authorperm", "timestamp" DESC);


DROP TABLE IF EXISTS "token_config";
CREATE TABLE "public"."token_config" (
    "token" character varying(20) NOT NULL,
    "cashout_window_days" integer DEFAULT '7' NOT NULL,
    "curation_reward_percentage" integer DEFAULT '75' NOT NULL,
    "author_curve_exponent" numeric DEFAULT '1' NOT NULL,
    "curation_curve_exponent" numeric DEFAULT '0.5' NOT NULL,
    "beneficiaries_reward_percentage" integer DEFAULT '0' NOT NULL,
    "beneficiaries_account" character varying(20) DEFAULT 'null' NOT NULL,
    "promoted_post_account" character varying(20) DEFAULT 'null' NOT NULL,
    "reward_pool_id" integer DEFAULT '0' NOT NULL,
    "token_account" character varying(20),
    "vote_regeneration_days" integer DEFAULT '5' NOT NULL,
    "downvote_regeneration_days" integer DEFAULT '5' NOT NULL,
    CONSTRAINT "token_config_symbol" PRIMARY KEY ("token")
) WITH (oids = false);


DROP TABLE IF EXISTS "votes";
CREATE TABLE "public"."votes" (
    "authorperm" character varying(300) NOT NULL,
    "voter" character varying(20) NOT NULL,
    "timestamp" timestamp NOT NULL,
    "token" character varying(20) NOT NULL,
    "rshares" numeric NOT NULL,
    "percent" smallint DEFAULT '0' NOT NULL,
    CONSTRAINT "votes_authorperm_token_voter" PRIMARY KEY ("authorperm", "token", "voter")
) WITH (oids = false);

CREATE INDEX "votes_authorperm_token_timestamp" ON "public"."votes" USING btree ("authorperm", "token", "timestamp");


-- 2021-05-27 14:08:39.878291+00
