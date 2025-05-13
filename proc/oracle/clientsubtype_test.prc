create or replace procedure rwa_dev.CLIENTSUBTYPE_TEST(DATAdate    in varchar2, --数据日期
                                               CLIENTNAME  in varchar2, ---客户名称
                                               REGISTSTATE in varchar2, ---所在国家地区
                                               msmbflag    in varchar2, ---工信部小微企业标识
                                               data        out sys_refcursor) is
  v_date        varchar2(20);
  v_CLIENTNAME  varchar2(32000);
  v_REGISTSTATE varchar2(20);
  v_msmbflag    varchar2(20);
  v_sql         VARCHAR2(32000);
begin
  delete from CLIENTSUBTYPE_TESTtb where 1 = 1;
  commit;
  if REGISTSTATE is null then
    v_REGISTSTATE := ' '' or 1=''1';
  else
    v_REGISTSTATE := REGISTSTATE;
  end if;

  if msmbflag is null then
    v_msmbflag := ' '' or 1=''1';
  else
    v_msmbflag := msmbflag;
  end if;

  v_date       := DATAdate;
  v_CLIENTNAME := CLIENTNAME;
  v_sql        := '
  insert into CLIENTSUBTYPE_TESTtb
  select distinct a.CLIENTNAME,
  a.REGISTSTATE,
  a.msmbflag,
  a.CLIENTTYPE,
  a.CLIENTSUBTYPE,b.itemname
  from rwa_dev.rwa_ei_client a 
  left join rwa.code_library b on a.clientsubtype=b.itemno and b.codeno=''ClientCategory''
 where a.datano = ''' || v_date || '''
   and a.clientname in(''' || v_CLIENTNAME || ''')
   and (a.REGISTSTATE=''' || v_REGISTSTATE || ''')
   and (a.msmbflag=''' || v_msmbflag || ''')
 ';
  EXECUTE IMMEDIATE v_sql;
  commit;

  open data for
    select * from CLIENTSUBTYPE_TESTtb;

end CLIENTSUBTYPE_TEST;
/

