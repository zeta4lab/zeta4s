-- zeta4s showcase local source/target schemas
-- Open-network local default Oracle image is gvenzl/oracle-free:23-slim and
-- provides the FREEPDB1 pluggable database.
-- Product validation target is Oracle 12c~26ai; version-specific differences
-- must be checked during migrated-job validation.
-- 컨테이너 첫 부팅 시 /container-entrypoint-initdb.d 디렉토리의 스크립트 실행

ALTER SESSION SET CONTAINER = FREEPDB1;

-- Oracle Free image 에서는 USERS 가 이미 있을 수 있으므로 없을 때만 생성한다.
DECLARE
  tablespace_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO tablespace_count
  FROM DBA_TABLESPACES
  WHERE TABLESPACE_NAME = 'USERS';

  IF tablespace_count = 0 THEN
    EXECUTE IMMEDIATE q'[
      CREATE TABLESPACE USERS
        DATAFILE '/opt/oracle/oradata/FREE/FREEPDB1/users01.dbf'
        SIZE 100M AUTOEXTEND ON NEXT 50M MAXSIZE UNLIMITED
        LOGGING ONLINE PERMANENT
        EXTENT MANAGEMENT LOCAL AUTOALLOCATE
        SEGMENT SPACE MANAGEMENT AUTO
    ]';
  END IF;
END;
/

ALTER PLUGGABLE DATABASE DEFAULT TABLESPACE USERS;

-- Source schema placeholder. Real migrated jobs can replace or extend this user.
CREATE USER showcase_src IDENTIFIED BY showcase_src
  DEFAULT TABLESPACE USERS
  QUOTA UNLIMITED ON USERS;

GRANT CONNECT, RESOURCE TO showcase_src;
GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE, CREATE VIEW TO showcase_src;
GRANT DBA TO showcase_src;

-- Target schema placeholder. Real migrated jobs can replace or extend this user.
CREATE USER showcase_tgt IDENTIFIED BY showcase_tgt
  DEFAULT TABLESPACE USERS
  QUOTA UNLIMITED ON USERS;

GRANT CONNECT, RESOURCE TO showcase_tgt;
GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE, CREATE VIEW TO showcase_tgt;
GRANT DBA TO showcase_tgt;

-- Target-side validation may need read access to source tables during migration checks.
GRANT SELECT ANY TABLE TO showcase_tgt;

-- Additional local Oracle schemas for migrated-job validation.
CREATE USER hubapp IDENTIFIED BY hubapp
  DEFAULT TABLESPACE USERS
  QUOTA UNLIMITED ON USERS;

GRANT CONNECT, RESOURCE TO hubapp;
GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE, CREATE VIEW TO hubapp;
GRANT DBA TO hubapp;

CREATE USER hubadm IDENTIFIED BY hubadm
  DEFAULT TABLESPACE USERS
  QUOTA UNLIMITED ON USERS;

GRANT CONNECT, RESOURCE TO hubadm;
GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE, CREATE VIEW TO hubadm;
GRANT DBA TO hubadm;

CREATE USER catalogadm IDENTIFIED BY catalogadm
  DEFAULT TABLESPACE USERS
  QUOTA UNLIMITED ON USERS;

GRANT CONNECT, RESOURCE TO catalogadm;
GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE, CREATE VIEW TO catalogadm;
GRANT DBA TO catalogadm;
