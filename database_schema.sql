-- Database Schema for Downloading and Parsing Common Crawl

create database commoncrawl character set 'utf8';
create user 'commoncrawl'@'%' identified by 'XXXXX';

create table wetpaths (
    id int not null auto_increment,
    path varchar(255) not null,
    processed int,
    primary key (id)
) engine innodb;
