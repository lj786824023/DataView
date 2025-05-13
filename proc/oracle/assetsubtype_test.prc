create or replace procedure rwa_dev.ASSETSUBTYPE_TEST(DATAdate         in varchar2, --数据日期
                                              Accsubject1      in varchar2, ---科目号
                                              clientsubtype    in varchar2, ---主体类型
                                              ORIGINALMATURITY in varchar2, ---原始期限
                                              BUSINESSTYPEID   in varchar2, ---业务品种代码
                                              data             out sys_refcursor) is
  v_date             varchar2(20);
  v_Accsubject1      varchar2(200);
  v_clientsubtype    varchar2(20);
  v_ORIGINALMATURITY varchar2(20);
  v_BUSINESSTYPEID   varchar2(20);
  v_sql              VARCHAR2(32000);
begin
  delete from assetsubtype_testtb where 1 = 1;
  commit;
  if clientsubtype is null then
    v_clientsubtype := ' '' or 1=''1';
    else v_clientsubtype := clientsubtype;
  end if;
  
    if ORIGINALMATURITY is null then
    v_ORIGINALMATURITY := ' '' or 1=''1';
    else v_ORIGINALMATURITY :=ORIGINALMATURITY;
  end if;
  
    if BUSINESSTYPEID is null then
    v_BUSINESSTYPEID := ' '' or 1=''1';
    else v_BUSINESSTYPEID :=BUSINESSTYPEID;
  end if;

  v_date        := DATAdate;
  v_Accsubject1 := Accsubject1;
  v_sql         := '
  insert into assetsubtype_testtb
    SELECT distinct A.Accsubject1,
                    b.clientsubtype,
                    A.ASSETSUBTYPE,
                    A.ORIGINALMATURITY,
                    A.BUSINESSTYPEID
      FROM RWA_EI_EXPOSURE A
     INNER JOIN RWA_EI_CLIENT B
        ON A.CLIENTID = B.CLIENTID
     WHERE A.DATANO =''' || v_date || '''
       AND B.DATANO = ''' || v_date || '''
       AND A.ACCSUBJECT1 in(''' || v_Accsubject1 || ''')
       and  (b.clientsubtype = ''' || v_clientsubtype || ''')
       and  (a.ORIGINALMATURITY = ''' || v_ORIGINALMATURITY || ''')
       and  (a.BUSINESSTYPEID = ''' || v_BUSINESSTYPEID || ''')
 ';

  EXECUTE IMMEDIATE v_sql;
  commit;

  open data for
    select * from assetsubtype_testtb;

end ASSETSUBTYPE_TEST;
/

