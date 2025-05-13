create or replace procedure rwa_dev.EXPOSUBCLASSSTD_TEST(DATAdate    in varchar2, --��������
       ASSETSUBTYPE  in varchar2,--�ʲ�С��
       clientsubtype   in varchar2,--����С��
       RCERAGENCY   in varchar2,---�ⲿ��������
       rcerating  in varchar2,--�ⲿ�����ȼ�
       CLAIMSLEVEL  in varchar2,--ծȯ����
       BONDISSUEINTENT   in varchar2,--ծȯ����Ŀ��
       ORIGINALMATURITY  in varchar2,--ԭʼ����
       BUSINESSTYPEID  in varchar2,---ҵ��Ʒ��
       BUSINESSTYPENAME  in varchar2,---ҵ��Ʒ������
       EQUITYINVESTCAUSE  in varchar2,--��ȨͶ���γ�ԭ��
       accsubject1   in varchar2,---��Ŀ
       NSUREALPROPERTYFLAG   in varchar2,---�Ƿ�����ò�����
       AGING   in varchar2,---����
       SSMBFLAG  in varchar2,--��׼С΢��ҵ
       data   out sys_refcursor) is
       
  v_date             varchar2(20);
  v_ASSETSUBTYPE    varchar2(20);
  v_clientsubtype varchar2(20);
  v_RCERAGENCY   varchar2(20);
  v_rcerating  varchar2(20);
  v_CLAIMSLEVEL   varchar2(20);
  v_BONDISSUEINTENT   varchar2(20);
  v_ORIGINALMATURITY   varchar2(20);
  v_BUSINESSTYPEID    varchar2(20);
  v_BUSINESSTYPENAME   varchar2(20);
  v_EQUITYINVESTCAUSE  varchar2(20);
  v_accsubject1    varchar2(20);
  v_NSUREALPROPERTYFLAG  varchar2(20);
  v_AGING  varchar2(20);
  v_SSMBFLAG  varchar2(20);
  v_sql              VARCHAR2(32000);
begin
  delete from EXPOSUBCLASSSTD_testtb where 1 = 1;
  commit;
  
  if ASSETSUBTYPE is null then
    v_ASSETSUBTYPE := ' '' or 1=''1';
    else v_ASSETSUBTYPE := ASSETSUBTYPE;
  end if;

  if clientsubtype is null then
    v_clientsubtype := ' '' or 1=''1';
    else v_clientsubtype := clientsubtype;
  end if;

  if RCERAGENCY is null then
    v_RCERAGENCY := ' '' or 1=''1';
    else v_RCERAGENCY := RCERAGENCY;
  end if;
  
    if rcerating is null then
    v_rcerating := ' '' or 1=''1';
    else v_rcerating := rcerating;
  end if;
  
  
    if CLAIMSLEVEL is null then
    v_CLAIMSLEVEL := ' '' or 1=''1';
    else v_CLAIMSLEVEL := CLAIMSLEVEL;
  end if;
  
    if BONDISSUEINTENT is null then
    v_BONDISSUEINTENT := ' '' or 1=''1';
    else v_BONDISSUEINTENT := BONDISSUEINTENT;
  end if;
  
    if ORIGINALMATURITY is null then
    v_ORIGINALMATURITY := ' '' or 1=''1';
    else v_ORIGINALMATURITY := ORIGINALMATURITY;
  end if;
  
    if BUSINESSTYPEID is null then
    v_BUSINESSTYPEID := ' '' or 1=''1';
    else v_BUSINESSTYPEID := BUSINESSTYPEID;
  end if;
  
    if BUSINESSTYPENAME is null then
    v_BUSINESSTYPENAME := ' '' or 1=''1';
    else v_BUSINESSTYPENAME := BUSINESSTYPENAME;
  end if;
  
    if EQUITYINVESTCAUSE is null then
    v_EQUITYINVESTCAUSE := ' '' or 1=''1';
    else v_EQUITYINVESTCAUSE := EQUITYINVESTCAUSE;
  end if;
  
    if accsubject1 is null then
    v_accsubject1 := ' '' or 1=''1';
    else v_accsubject1 := accsubject1;
  end if;
  
    if NSUREALPROPERTYFLAG is null then
    v_NSUREALPROPERTYFLAG := ' '' or 1=''1';
    else v_NSUREALPROPERTYFLAG := NSUREALPROPERTYFLAG;
  end if;
  
      if AGING is null then
    v_AGING := ' '' or 1=''1';
    else v_AGING := AGING;
  end if;

     if SSMBFLAG is null then
    v_SSMBFLAG := ' '' or 1=''1';
    else v_SSMBFLAG := SSMBFLAG;
  end if;
  
 
  v_date        := DATAdate;

  v_sql         := '
  insert into EXPOSUBCLASSSTD_testtb
select a.EXPOSUBCLASSSTD,
       a.ASSETSUBTYPE,
       b.clientsubtype,
       b.RCERAGENCY,
       b.rcerating,
       a.CLAIMSLEVEL,
       a.BONDISSUEINTENT,
       a.ORIGINALMATURITY,
       a.BUSINESSTYPEID,
       a.BUSINESSTYPENAME,
       a.EQUITYINVESTCAUSE,
       a.accsubject1,
       a.NSUREALPROPERTYFLAG,
       a.AGING,
       b.SSMBFLAG
  from rwa_ei_exposure a
  left join rwa_ei_client b
    on a.clientid = b.clientid
    where a.datano=''' || v_date || '''
    and   b.datano=''' || v_date || '''
    and  ��a.ASSETSUBTYPE= ''' || v_ASSETSUBTYPE || ''')
    and   ��b.clientsubtype=''' || v_clientsubtype || ''')
    and   ��b.RCERAGENCY=''' || v_RCERAGENCY || ''')
    and   ��b.rcerating=''' || v_rcerating || ''')
    and   ��a.CLAIMSLEVEL=''' || v_CLAIMSLEVEL || ''')
    and  �� a.BONDISSUEINTENT=''' || v_BONDISSUEINTENT || ''')
    and   ��a.ORIGINALMATURITY=''' || v_ORIGINALMATURITY || ''')
    and   ��a.BUSINESSTYPEID=''' || v_BUSINESSTYPEID || ''')
    and   ��a.BUSINESSTYPENAME=''' || v_BUSINESSTYPENAME || ''')
    and  �� a.EQUITYINVESTCAUSE=''' || v_EQUITYINVESTCAUSE|| ''')
    and  �� a.accsubject1=''' || v_accsubject1 || ''')
    and  �� a.NSUREALPROPERTYFLAG=''' || v_NSUREALPROPERTYFLAG || ''')
    and  �� a.AGING=''' || v_AGING || ''')
    and  �� b.SSMBFLAG=''' || v_SSMBFLAG || ''')
 ';

  EXECUTE IMMEDIATE v_sql;
  commit;

  open data for
    select * from EXPOSUBCLASSSTD_testtb;

end EXPOSUBCLASSSTD_TEST;
/

