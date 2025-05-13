CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_XF_CONTRACT(
                            p_data_dt_str IN  VARCHAR2, --��������
                            p_po_rtncode  OUT VARCHAR2, --���ر��
                            p_po_rtnmsg   OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_XF_CONTRACT
    ʵ�ֹ���:���ѽ���ϵͳ-��ͬ��Ϣ(������Դ���ϵͳ����ͬ�����Ϣȫ������RWA���ѽ��ڽӿڱ��ͬ��Ϣ����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :xlpang
    ��дʱ��:2019-05-28
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RMPS_CQ_CONTRACT|��ͬ��Ϣ
    Դ  ��2 :RWA_DEV.RWA_CD_PAYTODW_ORG|ͳһ����������
    Դ  ��3 :
    Դ  ��4 :
    Դ  ��5 :

    Ŀ���1 :RWA_DEV.RWA_XF_CONTRACT|���ѽ��ں�ͬ��Ϣ��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_XF_CONTRACT';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count1 INTEGER;
  --v_count2 INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --���Ŀ����е�ԭ�м�¼
    --DELETE FROM RWA_DEV.RWA_XF_CONTRACT WHERE DATADATE=TO_DATE(p_data_dt_str,'yyyyMMdd');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_XF_CONTRACT';


    --2.���������������ݴ�Դ����뵽Ŀ�����-��ʹ�ö�ȡ�δʹ�ö��
    INSERT INTO RWA_DEV.RWA_XF_CONTRACT(
                DataDate                               --��������
                ,DataNo                                --������ˮ��
                ,ContractID                            --��ͬID
                ,SContractID                           --Դ��ͬID
                ,SSysID                                --ԴϵͳID
                ,ClientID                              --��������ID
                ,SOrgID                                --Դ����ID
                ,SOrgName                              --Դ��������
                ,ORGSORTNO                             --�������������
                ,OrgID                                 --��������ID
                ,OrgName                               --������������
                ,IndustryID                            --������ҵ����
                ,IndustryName                          --������ҵ����
                ,BusinessLine                          --����
                ,AssetType                             --�ʲ�����
                ,AssetSubType                          --�ʲ�С��
                ,BusinessTypeID                        --ҵ��Ʒ�ִ���
                ,BusinessTypeName                      --ҵ��Ʒ������
                ,CreditRiskDataType                    --���÷�����������
                ,StartDate                             --��ʼ����
                ,DueDate                               --��������
                ,OriginalMaturity                      --ԭʼ����
                ,ResidualM                             --ʣ������
                ,SettlementCurrency                    --�������
                ,ContractAmount                        --��ͬ�ܽ��
                ,NotExtractPart                        --��ͬδ��ȡ����
                ,UncondCancelFlag                      --�Ƿ����ʱ����������
                ,ABSUAFlag                             --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPoolID                             --֤ȯ���ʲ���ID
                ,GroupID                               --������
                ,GUARANTEETYPE                         --��Ҫ������ʽ
                ,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
    SELECT          DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                 --��������
                ,p_data_dt_str                               AS DataNo                   --������ˮ��
                ,T1.CONTRACT_NBR                                AS ContractID               --��ͬID
                ,T1.CONTRACT_NBR                                    AS SContractID              --Դ��ͬID
                ,'XJ'                                       AS SSysID                   --ԴϵͳID                 Ĭ�ϣ����ÿ�(XYK)
                ,T2.CORE_CUST_ID                                 AS ClientID                --��������ID
                ,T2.CORE_ACCT_ORG                                  AS SOrgID                   --Դ����ID
                ,T3.ORGNAME                              AS SOrgName                --Դ��������
                ,T2.CORE_ACCT_ORG                                   AS ORGSORTNO                --�������������
                ,T3.SORTNO                                  AS OrgID                    --��������ID
                ,T3.ORGNAME                               AS OrgName                 --������������
                ,''                                          AS IndustryID               --������ҵ����             Ĭ�ϣ���
                ,''                                          AS IndustryName             --������ҵ����             Ĭ�ϣ���
                ,'0301'                                        AS BusinessLine             --����                     ����ҵ��
                ,NULL                                       AS AssetType                --�ʲ�����                 Ĭ��
                ,NULL                                     AS AssetSubType             --�ʲ�С��                 Ĭ��
                ,'11103038'                                  AS BusinessTypeID           --ҵ��Ʒ�ִ���             ���ÿ�ҵ��
                ,'��e��'                                 AS BusinessTypeName         --ҵ��Ʒ������             �̶�ֵ�����ÿ�
                ,'02'                                        AS CreditRiskDataType       --���÷�����������         �̶�ֵ��һ������
                ,T2.PAY_FINISH_DATE                                AS StartDate                --��ʼ����
                ,T2.DD_EXPIR_DAY                                  AS DueDate                  --��������
                ,CASE
                 WHEN (TO_DATE(T2.DD_EXPIR_DAY, 'YYYYMMDD') - TO_DATE(T2.PAY_FINISH_DATE, 'YYYYMMDD')) / 365 < 0 THEN
                  0
                 ELSE
                    (TO_DATE(T2.DD_EXPIR_DAY, 'YYYYMMDD') - TO_DATE(T2.PAY_FINISH_DATE, 'YYYYMMDD')) / 365
                 END AS ORIGINALMATURITY -- ԭʼ����
                ,CASE
                   WHEN (TO_DATE(T2.DD_EXPIR_DAY, 'YYYYMMDD') - TO_DATE(T2.DATANO, 'YYYYMMDD')) / 365 < 0 THEN
                    0
                   ELSE
                    (TO_DATE(T2.DD_EXPIR_DAY, 'YYYYMMDD') - TO_DATE(T2.DATANO, 'YYYYMMDD')) / 365
                 END AS RESIDUALM -- ʣ������
                ,'CNY'                                       AS SettlementCurrency       --�������                 Ĭ�ϣ������
                ,T1.CONTR_PRIN                               AS ContractAmount           --��ͬ�ܽ��
                ,0                                           AS NotExtractPart              --��ͬδ��ȡ����
                ,'0'                                         AS UncondCancelFlag            --�Ƿ����ʱ����������     Ĭ�ϣ���
                ,'0'                                         AS ABSUAFlag                   --�ʲ�֤ȯ�������ʲ���ʶ   Ĭ�ϣ���
                ,''                                          AS ABSPoolID                   --֤ȯ���ʲ���ID           Ĭ�ϣ���
                ,''                                          AS GroupID                     --������                 RWAϵͳ��ֵ
                ,'005'                                       AS GUARANTEETYPE               --��Ҫ������ʽ
                ,NULL                                        AS ABSPROPORTION               --�ʲ�֤ȯ������
    FROM RWA_DEV.RMPS_CQ_CONTRACT T1
    INNER JOIN RWA_DEV.RMPS_CQ_LOAN T2 
           ON T1.CONTRACT_NBR = T2.CONTRACT_NBR
          AND T1.DATANO = T2.DATANO
    LEFT JOIN RWA.ORG_INFO T3
        ON T2.CORE_ACCT_ORG = T3.ORGID
    WHERE T1.DATANO = p_data_dt_str 
      AND T2.PRIN_BAL <> 0 --��Ч����µĺ�ͬ
      AND T2.TERMIN_DATE IS NOT NULL; --����ս����ڲ�Ϊ��
   
    
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_XF_CONTRACT',cascade => true);


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_XF_CONTRACT;


    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count1;

    --�����쳣
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '���ѽ���ϵͳ��ͬ��Ϣ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;

    RETURN;
END PRO_RWA_XF_CONTRACT;
/

