CREATE TABLE companies (
    company_name varchar(200),
    market varchar(200),
    funding_total integer,
    status varchar(20),
    country varchar(10),
    state varchar(10),
    city varchar(30),
    funding_rounds integer,
    founded_at date,
    first_funding_at date,
    last_funding_at date,
    PRIMARY KEY (company_name,market,city)
);

CREATE TABLE acquisitions (
    company_name varchar(200),
    acquirer_name varchar(200),
    acquirer_market varchar(200),
    acquirer_country varchar(10),
    acquirer_state varchar(10),
    acquirer_city varchar(30),
    acquired_at date,
    price_amount integer,
    PRIMARY KEY (company_name, acquirer_name)
);
