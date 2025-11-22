--
-- PostgreSQL database dump
--

-- Dumped from database version 15.13 (Debian 15.13-0+deb12u1)
-- Dumped by pg_dump version 15.13 (Debian 15.13-0+deb12u1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: data_class; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.data_class (
    type_no integer NOT NULL,
    type character varying(10) NOT NULL
);


ALTER TABLE public.data_class OWNER TO quill;

--
-- Name: data_class_type_no_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.data_class_type_no_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.data_class_type_no_seq OWNER TO quill;

--
-- Name: data_class_type_no_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.data_class_type_no_seq OWNED BY public.data_class.type_no;


--
-- Name: dbirth; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dbirth (
    id integer NOT NULL,
    edge_node_id text NOT NULL,
    device_id text NOT NULL,
    metric_name text NOT NULL,
    metric_timestamp bigint NOT NULL,
    data_type text NOT NULL,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.dbirth OWNER TO quill;

--
-- Name: dbirth_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.dbirth_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.dbirth_id_seq OWNER TO quill;

--
-- Name: dbirth_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.dbirth_id_seq OWNED BY public.dbirth.id;


--
-- Name: device; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.device (
    device_id_no integer NOT NULL,
    device_id character varying(30) NOT NULL,
    node_id_no integer
);


ALTER TABLE public.device OWNER TO quill;

--
-- Name: device_device_id_no_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.device_device_id_no_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.device_device_id_no_seq OWNER TO quill;

--
-- Name: device_device_id_no_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.device_device_id_no_seq OWNED BY public.device.device_id_no;


--
-- Name: edge_node; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.edge_node (
    node_id_no integer NOT NULL,
    edge_node_id character varying(30) NOT NULL,
    group_id_no integer
);


ALTER TABLE public.edge_node OWNER TO quill;

--
-- Name: edge_node_node_id_no_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.edge_node_node_id_no_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.edge_node_node_id_no_seq OWNER TO quill;

--
-- Name: edge_node_node_id_no_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.edge_node_node_id_no_seq OWNED BY public.edge_node.node_id_no;


--
-- Name: group_ref; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.group_ref (
    group_id_no integer NOT NULL,
    group_id character varying(30) NOT NULL
);


ALTER TABLE public.group_ref OWNER TO quill;

--
-- Name: group_ref_group_id_no_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.group_ref_group_id_no_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.group_ref_group_id_no_seq OWNER TO quill;

--
-- Name: group_ref_group_id_no_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.group_ref_group_id_no_seq OWNED BY public.group_ref.group_id_no;


--
-- Name: metric; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.metric (
    payload_timestamp bigint NOT NULL,
    metric_timestamp bigint NOT NULL,
    value double precision NOT NULL,
    device_id_no integer NOT NULL,
    node_id_no integer NOT NULL,
    group_id_no integer NOT NULL,
    type_no integer NOT NULL,
    metric_name_no integer NOT NULL
);


ALTER TABLE public.metric OWNER TO quill;

--
-- Name: metric_name; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.metric_name (
    metric_name_no integer NOT NULL,
    name character varying(255) NOT NULL,
    data_type character varying(10) DEFAULT 'NOT KNOWN'::character varying NOT NULL,
    device_id_no integer
);


ALTER TABLE public.metric_name OWNER TO quill;

--
-- Name: metric_name_metric_name_no_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.metric_name_metric_name_no_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.metric_name_metric_name_no_seq OWNER TO quill;

--
-- Name: metric_name_metric_name_no_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.metric_name_metric_name_no_seq OWNED BY public.metric_name.metric_name_no;


--
-- Name: metric_unclogged; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.metric_unclogged (
    payload_timestamp bigint NOT NULL,
    metric_timestamp bigint NOT NULL,
    value double precision NOT NULL,
    device_id_no integer NOT NULL,
    node_id_no integer NOT NULL,
    group_id_no integer NOT NULL,
    type_no integer NOT NULL,
    metric_name_no integer NOT NULL
);


ALTER TABLE public.metric_unclogged OWNER TO quill;

--
-- Name: nbirth; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.nbirth (
    id integer NOT NULL,
    edge_node_id text NOT NULL,
    metric_name text NOT NULL,
    metric_timestamp bigint NOT NULL,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.nbirth OWNER TO quill;

--
-- Name: nbirth_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.nbirth_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.nbirth_id_seq OWNER TO quill;

--
-- Name: nbirth_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.nbirth_id_seq OWNED BY public.nbirth.id;


--
-- Name: sensor_errors; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.sensor_errors (
    id integer NOT NULL,
    created_at timestamp without time zone NOT NULL,
    sensor_name text NOT NULL,
    reason text NOT NULL,
    raw_payload bytea,
    partition integer,
    kafka_offset bigint
);


ALTER TABLE public.sensor_errors OWNER TO quill;

--
-- Name: sensor_errors_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.sensor_errors_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.sensor_errors_id_seq OWNER TO quill;

--
-- Name: sensor_errors_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.sensor_errors_id_seq OWNED BY public.sensor_errors.id;


--
-- Name: data_class type_no; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.data_class ALTER COLUMN type_no SET DEFAULT nextval('public.data_class_type_no_seq'::regclass);


--
-- Name: dbirth id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dbirth ALTER COLUMN id SET DEFAULT nextval('public.dbirth_id_seq'::regclass);


--
-- Name: device device_id_no; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.device ALTER COLUMN device_id_no SET DEFAULT nextval('public.device_device_id_no_seq'::regclass);


--
-- Name: edge_node node_id_no; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.edge_node ALTER COLUMN node_id_no SET DEFAULT nextval('public.edge_node_node_id_no_seq'::regclass);


--
-- Name: group_ref group_id_no; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.group_ref ALTER COLUMN group_id_no SET DEFAULT nextval('public.group_ref_group_id_no_seq'::regclass);


--
-- Name: metric_name metric_name_no; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.metric_name ALTER COLUMN metric_name_no SET DEFAULT nextval('public.metric_name_metric_name_no_seq'::regclass);


--
-- Name: nbirth id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.nbirth ALTER COLUMN id SET DEFAULT nextval('public.nbirth_id_seq'::regclass);


--
-- Name: sensor_errors id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sensor_errors ALTER COLUMN id SET DEFAULT nextval('public.sensor_errors_id_seq'::regclass);


--
-- Name: data_class data_class_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.data_class
    ADD CONSTRAINT data_class_pkey PRIMARY KEY (type_no);


--
-- Name: data_class data_class_type_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.data_class
    ADD CONSTRAINT data_class_type_key UNIQUE (type);


--
-- Name: dbirth dbirth_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dbirth
    ADD CONSTRAINT dbirth_pkey PRIMARY KEY (id);


--
-- Name: device device_device_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.device
    ADD CONSTRAINT device_device_id_key UNIQUE (device_id);


--
-- Name: device device_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.device
    ADD CONSTRAINT device_pkey PRIMARY KEY (device_id_no);


--
-- Name: edge_node edge_node_edge_node_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.edge_node
    ADD CONSTRAINT edge_node_edge_node_id_key UNIQUE (edge_node_id);


--
-- Name: edge_node edge_node_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.edge_node
    ADD CONSTRAINT edge_node_pkey PRIMARY KEY (node_id_no);


--
-- Name: group_ref group_ref_group_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.group_ref
    ADD CONSTRAINT group_ref_group_id_key UNIQUE (group_id);


--
-- Name: group_ref group_ref_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.group_ref
    ADD CONSTRAINT group_ref_pkey PRIMARY KEY (group_id_no);


--
-- Name: metric_name metric_name_name_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.metric_name
    ADD CONSTRAINT metric_name_name_key UNIQUE (name);


--
-- Name: metric_name metric_name_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.metric_name
    ADD CONSTRAINT metric_name_pkey PRIMARY KEY (metric_name_no);


--
-- Name: nbirth nbirth_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.nbirth
    ADD CONSTRAINT nbirth_pkey PRIMARY KEY (id);


--
-- Name: sensor_errors sensor_errors_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sensor_errors
    ADD CONSTRAINT sensor_errors_pkey PRIMARY KEY (id);


--
-- Name: metric_metric_timestamp_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX metric_metric_timestamp_idx ON public.metric_unclogged USING btree (metric_timestamp DESC);


--
-- Name: device fk_device_edge_node; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.device
    ADD CONSTRAINT fk_device_edge_node FOREIGN KEY (node_id_no) REFERENCES public.edge_node(node_id_no);


--
-- Name: edge_node fk_edge_node_group; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.edge_node
    ADD CONSTRAINT fk_edge_node_group FOREIGN KEY (group_id_no) REFERENCES public.group_ref(group_id_no);


--
-- Name: metric_name fk_metric_name_device; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.metric_name
    ADD CONSTRAINT fk_metric_name_device FOREIGN KEY (device_id_no) REFERENCES public.device(device_id_no);


--
-- Name: metric metric_type_no_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.metric
    ADD CONSTRAINT metric_type_no_fkey FOREIGN KEY (type_no) REFERENCES public.data_class(type_no);


--
-- PostgreSQL database dump complete
--
