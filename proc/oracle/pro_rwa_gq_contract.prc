CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_GQ_CONTRACT(
                                                P_DATA_DT_STR IN    VARCHAR2,      --��������
                                                P_PO_RTNCODE  OUT   VARCHAR2,      --���ر��
                                                P_PO_RTNMSG   OUT   VARCHAR2       --��������
                                               )
  /*
    �洢��������:RWA_DEV.PRO_RWA_GQ_CONTRACT
    ʵ�ֹ���:��ȨͶ��-��ͬ,��ṹΪ��ͬ��
    ���ݿھ�:ȫ��
    ����Ƶ��:��ĩ
    ��  ��  :V1.0.0
    ��д��  :TANGLW
    ��дʱ��:2016-01-07
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ ��1  :RWA.RWA_EI_UNCONSFIINVEST                       |��ȨͶ��-���÷��ձ�¶��
    Դ ��2  :RWA.ORG_INFO                                    |������
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����)��
  */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  P_PRO_NAME VARCHAR2(200) := 'RWA_DEV.PRO_RWA_GQ_CONTRACT';
  --�����ж�ֵ����
  P_COUNT INTEGER;
  --�����쳣����
  P_RAISE EXCEPTION;

  BEGIN
    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || P_PRO_NAME || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'));

    --1.���Ŀ����е�ԭ�м�¼
    /*�����ȫ�����ݼ��������Ŀ���*/
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_GQ_CONTRACT';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    /*����Ŀ���*/
    INSERT INTO RWA_GQ_CONTRACT(
               DataDate                             --��������
              ,DataNo                               --������ˮ��
              ,ContractID                           --��ͬID
              ,SContractID                          --Դ��ͬID
              ,SSysID                               --ԴϵͳID
              ,ClientID                             --��������ID
              ,SOrgID                               --Դ����ID
              ,SOrgName                             --Դ��������
              ,OrgSortNo                            --�������������
              ,OrgID                                --��������ID
              ,OrgName                              --������������
              ,IndustryID                           --������ҵ����
              ,IndustryName                         --������ҵ����
              ,BusinessLine                         --ҵ������
              ,AssetType                            --�ʲ�����
              ,AssetSubType                         --�ʲ�С��
              ,BusinessTypeID                       --ҵ��Ʒ�ִ���
              ,BusinessTypeName                     --ҵ��Ʒ������
              ,CreditRiskDataType                   --���÷�����������
              ,StartDate                            --��ʼ����
              ,DueDate                              --��������
              ,OriginalMaturity                     --ԭʼ����
              ,ResidualM                            --ʣ������
              ,SettlementCurrency                   --�������
              ,ContractAmount                       --��ͬ�ܽ��
              ,NotExtractPart                       --��ͬδ��ȡ����
              ,UncondCancelFlag                     --�Ƿ����ʱ����������
              ,ABSUAFlag                            --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPoolID                            --֤ȯ���ʲ���ID
              ,GroupID                              --������
              ,GUARANTEETYPE                        --��Ҫ������ʽ
              ,ABSPROPORTION                        --�ʲ�֤ȯ������
      )
      SELECT
              TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                      AS DATADATE            --��������
              ,P_DATA_DT_STR                                         AS DATANO              --������ˮ��
              ,'GQ'||T1.SERIALNO                                     AS CONTRACTID          --��ͬ����
              ,''                                                    AS SCONTRACTID         --Դ��ͬ����
              ,'GQ'                                                  AS SSYSID              --Դϵͳ����
              ,T1.CUSTID1                                            AS CLIENTID            --�����������
              ,T1.ORGID                                              AS SORGID              --Դ����ID
              ,T2.ORGNAME                                            AS SORGNAME            --Դ��������
              ,T2.SORTNO                                             AS ORGSORTNO           --�������������
              ,T1.ORGID                                              AS ORGID               --��������ID
              ,T2.ORGNAME                                            AS ORGNAME             --������������
              ,''                                                    AS INDUSTRYID          --������ҵ����
              ,''                                                    AS INDUSTRYNAME        --������ҵ����
              ,T1.BUSINESSLINE                                       AS BUSINESSLINE        --����
              ,''                                                    AS ASSETTYPE           --�ʲ�����
              ,''                                                    AS ASSETSUBTYPE        --�ʲ�С��
              ,'109060'                                              AS BUSINESSTYPEID      --ҵ��Ʒ�ִ���              (�̶�ֵ'998')
              ,'��ȨͶ��'                                            AS BUSINESSTYPENAME    --ҵ��Ʒ������              (�̶�ֵ'��ȨͶ��')
              ,'01'                                                  AS CREDITRISKDATATYPE  --���÷�����������          (01 һ�������,02 һ������,03 ���׶���)
              ,TO_CHAR(TO_DATE(P_DATA_DT_STR,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                                     AS STARTDATE           --��ʼ����
              ,TO_CHAR(ADD_MONTHS(TO_DATE(P_DATA_DT_STR,'YYYY-MM-DD'),1),'YYYY-MM-DD')
                                                                     AS DUEDATE             --��������
              ,(ADD_MONTHS(TO_DATE(P_DATA_DT_STR,'YYYYMMDD'),1) - TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365
                                                                     AS ORIGINALMATURITY    --ԭʼ����
              ,(ADD_MONTHS(TO_DATE(P_DATA_DT_STR,'YYYYMMDD'),1) - TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365
                                                                     AS RESIDUALM           --ʣ������
              ,T1.CURRENCY                                           AS SETTLEMENTCURRENCY  --�������
              ,CASE WHEN SUBSTR(T1.EQUITYINVESTTYPE,1,2)='02' THEN T1.EQUITYINVESTAMOUNT
              ELSE CTOCINVESTAMOUNT END                              AS CONTRACTAMOUNT      --��ͬ�ܽ��               (��ȨͶ�ʽ��)
              ,0                                                     AS NOTEXTRACTPART      --��ͬδ��ȡ����           (Ĭ��Ϊ0)
              ,'0'                                                   AS UNCONDCANCELFLAG    --�Ƿ����ʱ����������     (Ĭ��Ϊ��,1��0��)
              ,'0'                                                   AS ABSUAFLAG           --�ʲ�֤ȯ�������ʲ���ʶ   (Ĭ��Ϊ��,1��0��)
              ,''                                                    AS ABSPOOLID           --֤ȯ���ʲ���ID
              ,''                                                    AS GROUPID             --������                 (Ĭ��Ϊ��)
              ,''                                                    AS GUARANTEETYPE       --��Ҫ������ʽ
              ,NULL                                                  AS ABSPROPORTION       --�ʲ�֤ȯ������
    FROM      RWA.RWA_EI_UNCONSFIINVEST T1                  --���ڹ�ȨͶ�ʲ�¼��
    LEFT JOIN RWA.ORG_INFO T2
    ON        T2.ORGID = T1.ORGID
    WHERE     T1.DATADATE = TO_DATE(p_data_dt_str,'YYYY-MM-DD')
    AND       T1.CONSOLIDATEFLAG = '0'
    AND       T1.EQUITYINVESTTYPE LIKE '03%'
    ;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_GQ_CONTRACT',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO P_COUNT FROM RWA_DEV.RWA_GQ_CONTRACT;
    DBMS_OUTPUT.PUT_LINE('RWA_XN_CONTRACT��ǰ��������ݼ�¼Ϊ:' || P_COUNT || '��');
    DBMS_OUTPUT.PUT_LINE('��ִ�� ' || P_PRO_NAME || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'));

    P_PO_RTNCODE := '1';
    P_PO_RTNMSG  := '�ɹ�-'||P_COUNT;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||SQLCODE||';������ϢΪ:'||SQLERRM||';��������Ϊ:'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        ROLLBACK;
        P_PO_RTNCODE := SQLCODE;
        P_PO_RTNMSG  := '��ȨͶ��-��ͬ(PRO_RWA_XN_CONTRACT)ETLת��ʧ�ܣ�'|| SQLERRM;
    RETURN;

END PRO_RWA_GQ_CONTRACT;
/

