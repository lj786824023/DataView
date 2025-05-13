CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_CONTRACT(
			 											p_data_dt_str	IN	VARCHAR2,		--��������
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_CONTRACT
    ʵ�ֹ���:���ܺ�ͬ��,�������к�ͬ����Ϣ
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2016-06-01
    ��  λ	:�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1	:RWA_DEV.RWA_XD_CONTRACT|�Ŵ���ͬ��
    Դ  ��2	:RWA_DEV.RWA_HG_CONTRACT|�ع���ͬ��
    Դ  ��3	:RWA_DEV.RWA_LC_CONTRACT|��ƺ�ͬ��
    Դ  ��4	:RWA_DEV.RWA_PJ_CONTRACT|Ʊ�ݺ�ͬ��
    Դ  ��5	:RWA_DEV.RWA_TY_CONTRACT|ͬҵ��ͬ��
    Դ  ��6	:RWA_DEV.RWA_TZ_CONTRACT|Ͷ�ʺ�ͬ��
    Դ  ��7	:RWA_DEV.RWA_XYK_CONTRACT|���ÿ���ͬ��
    Դ  ��8	:RWA_DEV.RWA_GQ_CONTRACT|��Ȩ��ͬ��
    Դ  ��9	:RWA_DEV.RWA_ABS_ISSURE_CONTRACT|�ʲ�֤ȯ�����л�����ͬ��
    Դ  ��10:RWA_DEV.RWA_DZ_CONTRACT|��ծ�ʲ���ͬ��
    Դ  ��11:RWA_DEV.RWA_ZX_CONTRACT|ֱ�����к�ͬ��
    Դ  ��12:RWA_DEV.RWA_YSP_CONTRACT|����Ʒҵ���ͬ��
    
    Ŀ���  :RWA_DEV.RWA_EI_CONTRACT|��ͬ���ܱ�
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����)��
    pxl  2019/05/08  ��������Ʒҵ����Ϣ��Ŀ���
  	*/
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_CONTRACT';
  --�����ж�ֵ����
  v_count INTEGER;
  --�����쳣����
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'YYYY-MM-DD hh24:mi:ss'));

    BEGIN
    --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_CONTRACT DROP PARTITION CONTRACT' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --�״η���truncate�����2149�쳣
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '���ܷ��պ�ͬ��Ϣ��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --����һ����ǰ�����µķ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_CONTRACT ADD PARTITION CONTRACT' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*�����Ŵ��ĺ�ͬ��Ϣ*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
                DATADATE                              --��������
              ,DATANO                                --������ˮ��
              ,CONTRACTID                            --��ͬID
              ,SCONTRACTID                           --Դ��ͬID
              ,SSYSID                                --ԴϵͳID
              ,CLIENTID                              --��������ID
              ,SORGID                                --Դ����ID
              ,SORGNAME                              --Դ��������
              ,ORGSORTNO                             --�������������
              ,ORGID                                 --��������ID
              ,ORGNAME                               --������������
              ,INDUSTRYID                            --������ҵ����
              ,INDUSTRYNAME                          --������ҵ����
              ,BUSINESSLINE                          --����
              ,ASSETTYPE                             --�ʲ�����
              ,ASSETSUBTYPE                          --�ʲ�С��
              ,BUSINESSTYPEID                        --ҵ��Ʒ�ִ���
              ,BUSINESSTYPENAME                      --ҵ��Ʒ������
              ,CREDITRISKDATATYPE                    --���÷�����������
              ,STARTDATE                             --��ʼ����
              ,DUEDATE                               --��������
              ,ORIGINALMATURITY                      --ԭʼ����
              ,RESIDUALM                             --ʣ������
              ,SETTLEMENTCURRENCY                    --�������
              ,CONTRACTAMOUNT                        --��ͬ�ܽ��
              ,NOTEXTRACTPART                        --��ͬδ��ȡ����
              ,UNCONDCANCELFLAG                      --�Ƿ����ʱ����������
              ,ABSUAFLAG                             --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID                             --֤ȯ���ʲ���ID
              ,GROUPID                               --������
              ,GUARANTEETYPE                         --��Ҫ������ʽ
              ,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
     SELECT
                 DATADATE                                               AS DATADATE             --��������
                ,DATANO                                                 AS DATANO               --������ˮ��
                ,CONTRACTID                                             AS CONTRACTID           --��ͬID
                ,SCONTRACTID                                            AS SCONTRACTID          --Դ��ͬID
                ,SSYSID                                                 AS SSYSID               --ԴϵͳID
                ,CLIENTID                                               AS CLIENTID             --��������ID
                ,SORGID                                                 AS SORGID               --Դ����ID
                ,SORGNAME                                               AS SORGNAME             --Դ��������
                ,ORGSORTNO                                              AS ORGSORTNO            --�������������
                ,ORGID                                                  AS ORGID                --��������ID
                ,ORGNAME                                                AS ORGNAME              --������������
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --������ҵ����
                ,NVL(INDUSTRYNAME,'δ֪'��                              AS INDUSTRYNAME         --������ҵ����
                ,BUSINESSLINE                                           AS BUSINESSLINE         --����
                ,ASSETTYPE                                              AS ASSETTYPE            --�ʲ�����
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --�ʲ�С��
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --ҵ��Ʒ�ִ���
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --ҵ��Ʒ������
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --���÷�����������
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --��������
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --ԭʼ����
                ,RESIDUALM                                              AS RESIDUALM            --ʣ������
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --�������
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --��ͬ�ܽ��
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --��ͬδ��ȡ����
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --�Ƿ����ʱ����������
                ,ABSUAFLAG                                              AS ABSUAFLAG            --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPOOLID                                              AS ABSPOOLID            --�ʲ�֤ȯ����ID
                ,GROUPID                                                AS GROUPID              --������
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--��Ҫ������ʽ
                ,ABSPROPORTION                                          AS ABSPROPORTION        --�ʲ�֤ȯ������
    FROM 				RWA_DEV.RWA_XD_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*����ع��ĺ�ͬ��Ϣ*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
               DATADATE                              --��������
              ,DATANO                                --������ˮ��
              ,CONTRACTID                            --��ͬID
              ,SCONTRACTID                           --Դ��ͬID
              ,SSYSID                                --ԴϵͳID
              ,CLIENTID                              --��������ID
              ,SORGID                                --Դ����ID
              ,SORGNAME                              --Դ��������
              ,ORGSORTNO                             --�������������
              ,ORGID                                 --��������ID
              ,ORGNAME                               --������������
              ,INDUSTRYID                            --������ҵ����
              ,INDUSTRYNAME                          --������ҵ����
              ,BUSINESSLINE                          --����
              ,ASSETTYPE                             --�ʲ�����
              ,ASSETSUBTYPE                          --�ʲ�С��
              ,BUSINESSTYPEID                        --ҵ��Ʒ�ִ���
              ,BUSINESSTYPENAME                      --ҵ��Ʒ������
              ,CREDITRISKDATATYPE                    --���÷�����������
              ,STARTDATE                             --��ʼ����
              ,DUEDATE                               --��������
              ,ORIGINALMATURITY                      --ԭʼ����
              ,RESIDUALM                             --ʣ������
              ,SETTLEMENTCURRENCY                    --�������
              ,CONTRACTAMOUNT                        --��ͬ�ܽ��
              ,NOTEXTRACTPART                        --��ͬδ��ȡ����
              ,UNCONDCANCELFLAG                      --�Ƿ����ʱ����������
              ,ABSUAFLAG                             --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID                             --֤ȯ���ʲ���ID
              ,GROUPID                               --������
              ,GUARANTEETYPE                         --��Ҫ������ʽ
              ,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
     SELECT
                 DATADATE                                               AS DATADATE             --��������
                ,DATANO                                                 AS DATANO               --������ˮ��
                ,CONTRACTID                                             AS CONTRACTID           --��ͬID
                ,SCONTRACTID                                            AS SCONTRACTID          --Դ��ͬID
                ,SSYSID                                                 AS SSYSID               --ԴϵͳID
                ,CLIENTID                                               AS CLIENTID             --��������ID
                ,SORGID                                                 AS SORGID               --Դ����ID
                ,SORGNAME                                               AS SORGNAME             --Դ��������
                ,ORGSORTNO                                              AS ORGSORTNO            --�������������
                ,ORGID                                                  AS ORGID                --��������ID
                ,ORGNAME                                                AS ORGNAME              --������������
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --������ҵ����
                ,NVL(INDUSTRYNAME,'δ֪'��                              AS INDUSTRYNAME         --������ҵ����
                ,BUSINESSLINE                                           AS BUSINESSLINE         --����
                ,ASSETTYPE                                              AS ASSETTYPE            --�ʲ�����
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --�ʲ�С��
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --ҵ��Ʒ�ִ���
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --ҵ��Ʒ������
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --���÷�����������
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --��������
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --ԭʼ����
                ,RESIDUALM                                              AS RESIDUALM            --ʣ������
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --�������
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --��ͬ�ܽ��
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --��ͬδ��ȡ����
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --�Ƿ����ʱ����������
                ,ABSUAFLAG                                              AS ABSUAFLAG            --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPOOLID                                              AS ABSPOOLID            --�ʲ�֤ȯ����ID
                ,GROUPID                                                AS GROUPID              --������
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--��Ҫ������ʽ
                ,ABSPROPORTION                                          AS ABSPROPORTION        --�ʲ�֤ȯ������
    FROM 				RWA_DEV.RWA_HG_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*������Ƶĺ�ͬ��Ϣ*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
                DATADATE                              --��������
              ,DATANO                                --������ˮ��
              ,CONTRACTID                            --��ͬID
              ,SCONTRACTID                           --Դ��ͬID
              ,SSYSID                                --ԴϵͳID
              ,CLIENTID                              --��������ID
              ,SORGID                                --Դ����ID
              ,SORGNAME                              --Դ��������
              ,ORGSORTNO                             --�������������
              ,ORGID                                 --��������ID
              ,ORGNAME                               --������������
              ,INDUSTRYID                            --������ҵ����
              ,INDUSTRYNAME                          --������ҵ����
              ,BUSINESSLINE                          --����
              ,ASSETTYPE                             --�ʲ�����
              ,ASSETSUBTYPE                          --�ʲ�С��
              ,BUSINESSTYPEID                        --ҵ��Ʒ�ִ���
              ,BUSINESSTYPENAME                      --ҵ��Ʒ������
              ,CREDITRISKDATATYPE                    --���÷�����������
              ,STARTDATE                             --��ʼ����
              ,DUEDATE                               --��������
              ,ORIGINALMATURITY                      --ԭʼ����
              ,RESIDUALM                             --ʣ������
              ,SETTLEMENTCURRENCY                    --�������
              ,CONTRACTAMOUNT                        --��ͬ�ܽ��
              ,NOTEXTRACTPART                        --��ͬδ��ȡ����
              ,UNCONDCANCELFLAG                      --�Ƿ����ʱ����������
              ,ABSUAFLAG                             --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID                             --֤ȯ���ʲ���ID
              ,GROUPID                               --������
              ,GUARANTEETYPE                         --��Ҫ������ʽ
              ,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
     SELECT
                 DATADATE                                               AS DATADATE             --��������
                ,DATANO                                                 AS DATANO               --������ˮ��
                ,CONTRACTID                                             AS CONTRACTID           --��ͬID
                ,SCONTRACTID                                            AS SCONTRACTID          --Դ��ͬID
                ,SSYSID                                                 AS SSYSID               --ԴϵͳID
                ,CLIENTID                                               AS CLIENTID             --��������ID
                ,SORGID                                                 AS SORGID               --Դ����ID
                ,SORGNAME                                               AS SORGNAME             --Դ��������
                ,ORGSORTNO                                              AS ORGSORTNO            --�������������
                ,ORGID                                                  AS ORGID                --��������ID
                ,ORGNAME                                                AS ORGNAME              --������������
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --������ҵ����
                ,NVL(INDUSTRYNAME,'δ֪'��                              AS INDUSTRYNAME         --������ҵ����
                ,BUSINESSLINE                                           AS BUSINESSLINE         --����
                ,ASSETTYPE                                              AS ASSETTYPE            --�ʲ�����
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --�ʲ�С��
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --ҵ��Ʒ�ִ���
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --ҵ��Ʒ������
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --���÷�����������
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --��������
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --ԭʼ����
                ,RESIDUALM                                              AS RESIDUALM            --ʣ������
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --�������
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --��ͬ�ܽ��
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --��ͬδ��ȡ����
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --�Ƿ����ʱ����������
                ,ABSUAFLAG                                              AS ABSUAFLAG            --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPOOLID                                              AS ABSPOOLID            --�ʲ�֤ȯ����ID
                ,GROUPID                                                AS GROUPID              --������
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--��Ҫ������ʽ
                ,ABSPROPORTION                                          AS ABSPROPORTION        --�ʲ�֤ȯ������
    FROM 				RWA_DEV.RWA_LC_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*����Ʊ�ݵĺ�ͬ��Ϣ*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
                DATADATE                              --��������
              ,DATANO                                --������ˮ��
              ,CONTRACTID                            --��ͬID
              ,SCONTRACTID                           --Դ��ͬID
              ,SSYSID                                --ԴϵͳID
              ,CLIENTID                              --��������ID
              ,SORGID                                --Դ����ID
              ,SORGNAME                              --Դ��������
              ,ORGSORTNO                             --�������������
              ,ORGID                                 --��������ID
              ,ORGNAME                               --������������
              ,INDUSTRYID                            --������ҵ����
              ,INDUSTRYNAME                          --������ҵ����
              ,BUSINESSLINE                          --����
              ,ASSETTYPE                             --�ʲ�����
              ,ASSETSUBTYPE                          --�ʲ�С��
              ,BUSINESSTYPEID                        --ҵ��Ʒ�ִ���
              ,BUSINESSTYPENAME                      --ҵ��Ʒ������
              ,CREDITRISKDATATYPE                    --���÷�����������
              ,STARTDATE                             --��ʼ����
              ,DUEDATE                               --��������
              ,ORIGINALMATURITY                      --ԭʼ����
              ,RESIDUALM                             --ʣ������
              ,SETTLEMENTCURRENCY                    --�������
              ,CONTRACTAMOUNT                        --��ͬ�ܽ��
              ,NOTEXTRACTPART                        --��ͬδ��ȡ����
              ,UNCONDCANCELFLAG                      --�Ƿ����ʱ����������
              ,ABSUAFLAG                             --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID                             --֤ȯ���ʲ���ID
              ,GROUPID                               --������
              ,GUARANTEETYPE                         --��Ҫ������ʽ
              ,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
     SELECT
                 DATADATE                                               AS DATADATE             --��������
                ,DATANO                                                 AS DATANO               --������ˮ��
                ,CONTRACTID                                             AS CONTRACTID           --��ͬID
                ,SCONTRACTID                                            AS SCONTRACTID          --Դ��ͬID
                ,SSYSID                                                 AS SSYSID               --ԴϵͳID
                ,CLIENTID                                               AS CLIENTID             --��������ID
                ,SORGID                                                 AS SORGID               --Դ����ID
                ,SORGNAME                                               AS SORGNAME             --Դ��������
                ,ORGSORTNO                                              AS ORGSORTNO            --�������������
                ,ORGID                                                  AS ORGID                --��������ID
                ,ORGNAME                                                AS ORGNAME              --������������
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --������ҵ����
                ,NVL(INDUSTRYNAME,'δ֪'��                              AS INDUSTRYNAME         --������ҵ����
                ,BUSINESSLINE                                           AS BUSINESSLINE         --����
                ,ASSETTYPE                                              AS ASSETTYPE            --�ʲ�����
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --�ʲ�С��
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --ҵ��Ʒ�ִ���
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --ҵ��Ʒ������
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --���÷�����������
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --��������
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --ԭʼ����
                ,RESIDUALM                                              AS RESIDUALM            --ʣ������
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --�������
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --��ͬ�ܽ��
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --��ͬδ��ȡ����
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --�Ƿ����ʱ����������
                ,ABSUAFLAG                                              AS ABSUAFLAG            --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPOOLID                                              AS ABSPOOLID            --�ʲ�֤ȯ����ID
                ,GROUPID                                                AS GROUPID              --������
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--��Ҫ������ʽ
                ,ABSPROPORTION                                          AS ABSPROPORTION        --�ʲ�֤ȯ������
    FROM 				RWA_DEV.RWA_PJ_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*����ͬҵ�ĺ�ͬ��Ϣ*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
                DATADATE                              --��������
              ,DATANO                                --������ˮ��
              ,CONTRACTID                            --��ͬID
              ,SCONTRACTID                           --Դ��ͬID
              ,SSYSID                                --ԴϵͳID
              ,CLIENTID                              --��������ID
              ,SORGID                                --Դ����ID
              ,SORGNAME                              --Դ��������
              ,ORGSORTNO                             --�������������
              ,ORGID                                 --��������ID
              ,ORGNAME                               --������������
              ,INDUSTRYID                            --������ҵ����
              ,INDUSTRYNAME                          --������ҵ����
              ,BUSINESSLINE                          --����
              ,ASSETTYPE                             --�ʲ�����
              ,ASSETSUBTYPE                          --�ʲ�С��
              ,BUSINESSTYPEID                        --ҵ��Ʒ�ִ���
              ,BUSINESSTYPENAME                      --ҵ��Ʒ������
              ,CREDITRISKDATATYPE                    --���÷�����������
              ,STARTDATE                             --��ʼ����
              ,DUEDATE                               --��������
              ,ORIGINALMATURITY                      --ԭʼ����
              ,RESIDUALM                             --ʣ������
              ,SETTLEMENTCURRENCY                    --�������
              ,CONTRACTAMOUNT                        --��ͬ�ܽ��
              ,NOTEXTRACTPART                        --��ͬδ��ȡ����
              ,UNCONDCANCELFLAG                      --�Ƿ����ʱ����������
              ,ABSUAFLAG                             --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID                             --֤ȯ���ʲ���ID
              ,GROUPID                               --������
              ,GUARANTEETYPE                         --��Ҫ������ʽ
              ,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
     SELECT
                 DATADATE                                               AS DATADATE             --��������
                ,DATANO                                                 AS DATANO               --������ˮ��
                ,CONTRACTID                                             AS CONTRACTID           --��ͬID
                ,SCONTRACTID                                            AS SCONTRACTID          --Դ��ͬID
                ,SSYSID                                                 AS SSYSID               --ԴϵͳID
                ,CLIENTID                                               AS CLIENTID             --��������ID
                ,SORGID                                                 AS SORGID               --Դ����ID
                ,SORGNAME                                               AS SORGNAME             --Դ��������
                ,ORGSORTNO                                              AS ORGSORTNO            --�������������
                ,ORGID                                                  AS ORGID                --��������ID
                ,ORGNAME                                                AS ORGNAME              --������������
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --������ҵ����
                ,NVL(INDUSTRYNAME,'δ֪'��                              AS INDUSTRYNAME         --������ҵ����
                ,BUSINESSLINE                                           AS BUSINESSLINE         --����
                ,ASSETTYPE                                              AS ASSETTYPE            --�ʲ�����
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --�ʲ�С��
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --ҵ��Ʒ�ִ���
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --ҵ��Ʒ������
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --���÷�����������
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --��������
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --ԭʼ����
                ,RESIDUALM                                              AS RESIDUALM            --ʣ������
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --�������
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --��ͬ�ܽ��
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --��ͬδ��ȡ����
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --�Ƿ����ʱ����������
                ,ABSUAFLAG                                              AS ABSUAFLAG            --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPOOLID                                              AS ABSPOOLID            --�ʲ�֤ȯ����ID
                ,GROUPID                                                AS GROUPID              --������
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--��Ҫ������ʽ
                ,ABSPROPORTION                                          AS ABSPROPORTION        --�ʲ�֤ȯ������
    FROM 				RWA_DEV.RWA_TY_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*����Ͷ�ʵĺ�ͬ��Ϣ*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
                DATADATE                              --��������
              ,DATANO                                --������ˮ��
              ,CONTRACTID                            --��ͬID
              ,SCONTRACTID                           --Դ��ͬID
              ,SSYSID                                --ԴϵͳID
              ,CLIENTID                              --��������ID
              ,SORGID                                --Դ����ID
              ,SORGNAME                              --Դ��������
              ,ORGSORTNO                             --�������������
              ,ORGID                                 --��������ID
              ,ORGNAME                               --������������
              ,INDUSTRYID                            --������ҵ����
              ,INDUSTRYNAME                          --������ҵ����
              ,BUSINESSLINE                          --����
              ,ASSETTYPE                             --�ʲ�����
              ,ASSETSUBTYPE                          --�ʲ�С��
              ,BUSINESSTYPEID                        --ҵ��Ʒ�ִ���
              ,BUSINESSTYPENAME                      --ҵ��Ʒ������
              ,CREDITRISKDATATYPE                    --���÷�����������
              ,STARTDATE                             --��ʼ����
              ,DUEDATE                               --��������
              ,ORIGINALMATURITY                      --ԭʼ����
              ,RESIDUALM                             --ʣ������
              ,SETTLEMENTCURRENCY                    --�������
              ,CONTRACTAMOUNT                        --��ͬ�ܽ��
              ,NOTEXTRACTPART                        --��ͬδ��ȡ����
              ,UNCONDCANCELFLAG                      --�Ƿ����ʱ����������
              ,ABSUAFLAG                             --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID                             --֤ȯ���ʲ���ID
              ,GROUPID                               --������
              ,GUARANTEETYPE                         --��Ҫ������ʽ
              ,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
     SELECT
                 DATADATE                                               AS DATADATE             --��������
                ,DATANO                                                 AS DATANO               --������ˮ��
                ,CONTRACTID                                             AS CONTRACTID           --��ͬID
                ,SCONTRACTID                                            AS SCONTRACTID          --Դ��ͬID
                ,SSYSID                                                 AS SSYSID               --ԴϵͳID
                ,CLIENTID                                               AS CLIENTID             --��������ID
                ,SORGID                                                 AS SORGID               --Դ����ID
                ,SORGNAME                                               AS SORGNAME             --Դ��������
                ,ORGSORTNO                                              AS ORGSORTNO            --�������������
                ,ORGID                                                  AS ORGID                --��������ID
                ,ORGNAME                                                AS ORGNAME              --������������
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --������ҵ����
                ,NVL(INDUSTRYNAME,'δ֪'��                              AS INDUSTRYNAME         --������ҵ����
                ,BUSINESSLINE                                           AS BUSINESSLINE         --����
                ,ASSETTYPE                                              AS ASSETTYPE            --�ʲ�����
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --�ʲ�С��
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --ҵ��Ʒ�ִ���
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --ҵ��Ʒ������
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --���÷�����������
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --��������
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --ԭʼ����
                ,RESIDUALM                                              AS RESIDUALM            --ʣ������
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --�������
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --��ͬ�ܽ��
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --��ͬδ��ȡ����
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --�Ƿ����ʱ����������
                ,ABSUAFLAG                                              AS ABSUAFLAG            --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPOOLID                                              AS ABSPOOLID            --�ʲ�֤ȯ����ID
                ,GROUPID                                                AS GROUPID              --������
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--��Ҫ������ʽ
                ,ABSPROPORTION                                          AS ABSPROPORTION        --�ʲ�֤ȯ������
    FROM 				RWA_DEV.RWA_TZ_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*�������ÿ��ĺ�ͬ��Ϣ*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
                DATADATE                              --��������
              ,DATANO                                --������ˮ��
              ,CONTRACTID                            --��ͬID
              ,SCONTRACTID                           --Դ��ͬID
              ,SSYSID                                --ԴϵͳID
              ,CLIENTID                              --��������ID
              ,SORGID                                --Դ����ID
              ,SORGNAME                              --Դ��������
              ,ORGSORTNO                             --�������������
              ,ORGID                                 --��������ID
              ,ORGNAME                               --������������
              ,INDUSTRYID                            --������ҵ����
              ,INDUSTRYNAME                          --������ҵ����
              ,BUSINESSLINE                          --����
              ,ASSETTYPE                             --�ʲ�����
              ,ASSETSUBTYPE                          --�ʲ�С��
              ,BUSINESSTYPEID                        --ҵ��Ʒ�ִ���
              ,BUSINESSTYPENAME                      --ҵ��Ʒ������
              ,CREDITRISKDATATYPE                    --���÷�����������
              ,STARTDATE                             --��ʼ����
              ,DUEDATE                               --��������
              ,ORIGINALMATURITY                      --ԭʼ����
              ,RESIDUALM                             --ʣ������
              ,SETTLEMENTCURRENCY                    --�������
              ,CONTRACTAMOUNT                        --��ͬ�ܽ��
              ,NOTEXTRACTPART                        --��ͬδ��ȡ����
              ,UNCONDCANCELFLAG                      --�Ƿ����ʱ����������
              ,ABSUAFLAG                             --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID                             --֤ȯ���ʲ���ID
              ,GROUPID                               --������
              ,GUARANTEETYPE                         --��Ҫ������ʽ
              ,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
     SELECT
                 DATADATE                                               AS DATADATE             --��������
                ,DATANO                                                 AS DATANO               --������ˮ��
                ,CONTRACTID                                             AS CONTRACTID           --��ͬID
                ,SCONTRACTID                                            AS SCONTRACTID          --Դ��ͬID
                ,SSYSID                                                 AS SSYSID               --ԴϵͳID
                ,CLIENTID                                               AS CLIENTID             --��������ID
                ,SORGID                                                 AS SORGID               --Դ����ID
                ,SORGNAME                                               AS SORGNAME             --Դ��������
                ,ORGSORTNO                                              AS ORGSORTNO            --�������������
                ,ORGID                                                  AS ORGID                --��������ID
                ,ORGNAME                                                AS ORGNAME              --������������
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --������ҵ����
                ,NVL(INDUSTRYNAME,'δ֪'��                              AS INDUSTRYNAME         --������ҵ����
                ,BUSINESSLINE                                           AS BUSINESSLINE         --����
                ,ASSETTYPE                                              AS ASSETTYPE            --�ʲ�����
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --�ʲ�С��
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --ҵ��Ʒ�ִ���
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --ҵ��Ʒ������
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --���÷�����������
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --��������
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --ԭʼ����
                ,RESIDUALM                                              AS RESIDUALM            --ʣ������
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --�������
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --��ͬ�ܽ��
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --��ͬδ��ȡ����
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --�Ƿ����ʱ����������
                ,ABSUAFLAG                                              AS ABSUAFLAG            --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPOOLID                                              AS ABSPOOLID            --�ʲ�֤ȯ����ID
                ,GROUPID                                                AS GROUPID              --������
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--��Ҫ������ʽ
                ,ABSPROPORTION                                          AS ABSPROPORTION        --�ʲ�֤ȯ������
    FROM 				RWA_DEV.RWA_XYK_CONTRACT A
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND NOT EXISTS(SELECT 1 FROM RWA_TZ_CONTRACT T WHERE A.CONTRACTID=T.CONTRACTID 
        AND T.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') )
    ;
    COMMIT;


    /*�����Ȩ�ĺ�ͬ��Ϣ*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
                DATADATE                              --��������
              ,DATANO                                --������ˮ��
              ,CONTRACTID                            --��ͬID
              ,SCONTRACTID                           --Դ��ͬID
              ,SSYSID                                --ԴϵͳID
              ,CLIENTID                              --��������ID
              ,SORGID                                --Դ����ID
              ,SORGNAME                              --Դ��������
              ,ORGSORTNO                             --�������������
              ,ORGID                                 --��������ID
              ,ORGNAME                               --������������
              ,INDUSTRYID                            --������ҵ����
              ,INDUSTRYNAME                          --������ҵ����
              ,BUSINESSLINE                          --����
              ,ASSETTYPE                             --�ʲ�����
              ,ASSETSUBTYPE                          --�ʲ�С��
              ,BUSINESSTYPEID                        --ҵ��Ʒ�ִ���
              ,BUSINESSTYPENAME                      --ҵ��Ʒ������
              ,CREDITRISKDATATYPE                    --���÷�����������
              ,STARTDATE                             --��ʼ����
              ,DUEDATE                               --��������
              ,ORIGINALMATURITY                      --ԭʼ����
              ,RESIDUALM                             --ʣ������
              ,SETTLEMENTCURRENCY                    --�������
              ,CONTRACTAMOUNT                        --��ͬ�ܽ��
              ,NOTEXTRACTPART                        --��ͬδ��ȡ����
              ,UNCONDCANCELFLAG                      --�Ƿ����ʱ����������
              ,ABSUAFLAG                             --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID                             --֤ȯ���ʲ���ID
              ,GROUPID                               --������
              ,GUARANTEETYPE                         --��Ҫ������ʽ
              ,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
     SELECT
                 DATADATE                                               AS DATADATE             --��������
                ,DATANO                                                 AS DATANO               --������ˮ��
                ,CONTRACTID                                             AS CONTRACTID           --��ͬID
                ,SCONTRACTID                                            AS SCONTRACTID          --Դ��ͬID
                ,SSYSID                                                 AS SSYSID               --ԴϵͳID
                ,CLIENTID                                               AS CLIENTID             --��������ID
                ,SORGID                                                 AS SORGID               --Դ����ID
                ,SORGNAME                                               AS SORGNAME             --Դ��������
                ,ORGSORTNO                                              AS ORGSORTNO            --�������������
                ,ORGID                                                  AS ORGID                --��������ID
                ,ORGNAME                                                AS ORGNAME              --������������
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --������ҵ����
                ,NVL(INDUSTRYNAME,'δ֪'��                              AS INDUSTRYNAME         --������ҵ����
                ,BUSINESSLINE                                           AS BUSINESSLINE         --����
                ,ASSETTYPE                                              AS ASSETTYPE            --�ʲ�����
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --�ʲ�С��
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --ҵ��Ʒ�ִ���
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --ҵ��Ʒ������
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --���÷�����������
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --��������
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --ԭʼ����
                ,RESIDUALM                                              AS RESIDUALM            --ʣ������
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --�������
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --��ͬ�ܽ��
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --��ͬδ��ȡ����
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --�Ƿ����ʱ����������
                ,ABSUAFLAG                                              AS ABSUAFLAG            --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPOOLID                                              AS ABSPOOLID            --�ʲ�֤ȯ����ID
                ,GROUPID                                                AS GROUPID              --������
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--��Ҫ������ʽ
                ,ABSPROPORTION                                          AS ABSPROPORTION        --�ʲ�֤ȯ������
    FROM 				RWA_DEV.RWA_GQ_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*�����ʲ�֤ȯ�����л����ĺ�ͬ��Ϣ*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
                DATADATE                              --��������
              ,DATANO                                --������ˮ��
              ,CONTRACTID                            --��ͬID
              ,SCONTRACTID                           --Դ��ͬID
              ,SSYSID                                --ԴϵͳID
              ,CLIENTID                              --��������ID
              ,SORGID                                --Դ����ID
              ,SORGNAME                              --Դ��������
              ,ORGSORTNO                             --�������������
              ,ORGID                                 --��������ID
              ,ORGNAME                               --������������
              ,INDUSTRYID                            --������ҵ����
              ,INDUSTRYNAME                          --������ҵ����
              ,BUSINESSLINE                          --����
              ,ASSETTYPE                             --�ʲ�����
              ,ASSETSUBTYPE                          --�ʲ�С��
              ,BUSINESSTYPEID                        --ҵ��Ʒ�ִ���
              ,BUSINESSTYPENAME                      --ҵ��Ʒ������
              ,CREDITRISKDATATYPE                    --���÷�����������
              ,STARTDATE                             --��ʼ����
              ,DUEDATE                               --��������
              ,ORIGINALMATURITY                      --ԭʼ����
              ,RESIDUALM                             --ʣ������
              ,SETTLEMENTCURRENCY                    --�������
              ,CONTRACTAMOUNT                        --��ͬ�ܽ��
              ,NOTEXTRACTPART                        --��ͬδ��ȡ����
              ,UNCONDCANCELFLAG                      --�Ƿ����ʱ����������
              ,ABSUAFLAG                             --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID                             --֤ȯ���ʲ���ID
              ,GROUPID                               --������
              ,GUARANTEETYPE                         --��Ҫ������ʽ
              ,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
     SELECT
                 DATADATE                                               AS DATADATE             --��������
                ,DATANO                                                 AS DATANO               --������ˮ��
                ,CONTRACTID                                             AS CONTRACTID           --��ͬID
                ,SCONTRACTID                                            AS SCONTRACTID          --Դ��ͬID
                ,SSYSID                                                 AS SSYSID               --ԴϵͳID
                ,CLIENTID                                               AS CLIENTID             --��������ID
                ,SORGID                                                 AS SORGID               --Դ����ID
                ,SORGNAME                                               AS SORGNAME             --Դ��������
                ,ORGSORTNO                                              AS ORGSORTNO            --�������������
                ,ORGID                                                  AS ORGID                --��������ID
                ,ORGNAME                                                AS ORGNAME              --������������
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --������ҵ����
                ,NVL(INDUSTRYNAME,'δ֪'��                              AS INDUSTRYNAME         --������ҵ����
                ,BUSINESSLINE                                           AS BUSINESSLINE         --����
                ,ASSETTYPE                                              AS ASSETTYPE            --�ʲ�����
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --�ʲ�С��
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --ҵ��Ʒ�ִ���
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --ҵ��Ʒ������
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --���÷�����������
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --��������
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --ԭʼ����
                ,RESIDUALM                                              AS RESIDUALM            --ʣ������
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --�������
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --��ͬ�ܽ��
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --��ͬδ��ȡ����
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --�Ƿ����ʱ����������
                ,ABSUAFLAG                                              AS ABSUAFLAG            --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPOOLID                                              AS ABSPOOLID            --�ʲ�֤ȯ����ID
                ,GROUPID                                                AS GROUPID              --������
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--��Ҫ������ʽ
                ,ABSPROPORTION                                          AS ABSPROPORTION        --�ʲ�֤ȯ������
    FROM 				RWA_DEV.RWA_ABS_ISSURE_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;

    /*�����ծ�ʲ��ĺ�ͬ��Ϣ*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
                DATADATE                              --��������
              ,DATANO                                --������ˮ��
              ,CONTRACTID                            --��ͬID
              ,SCONTRACTID                           --Դ��ͬID
              ,SSYSID                                --ԴϵͳID
              ,CLIENTID                              --��������ID
              ,SORGID                                --Դ����ID
              ,SORGNAME                              --Դ��������
              ,ORGSORTNO                             --�������������
              ,ORGID                                 --��������ID
              ,ORGNAME                               --������������
              ,INDUSTRYID                            --������ҵ����
              ,INDUSTRYNAME                          --������ҵ����
              ,BUSINESSLINE                          --����
              ,ASSETTYPE                             --�ʲ�����
              ,ASSETSUBTYPE                          --�ʲ�С��
              ,BUSINESSTYPEID                        --ҵ��Ʒ�ִ���
              ,BUSINESSTYPENAME                      --ҵ��Ʒ������
              ,CREDITRISKDATATYPE                    --���÷�����������
              ,STARTDATE                             --��ʼ����
              ,DUEDATE                               --��������
              ,ORIGINALMATURITY                      --ԭʼ����
              ,RESIDUALM                             --ʣ������
              ,SETTLEMENTCURRENCY                    --�������
              ,CONTRACTAMOUNT                        --��ͬ�ܽ��
              ,NOTEXTRACTPART                        --��ͬδ��ȡ����
              ,UNCONDCANCELFLAG                      --�Ƿ����ʱ����������
              ,ABSUAFLAG                             --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID                             --֤ȯ���ʲ���ID
              ,GROUPID                               --������
              ,GUARANTEETYPE                         --��Ҫ������ʽ
              ,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
     SELECT
                 DATADATE                                               AS DATADATE             --��������
                ,DATANO                                                 AS DATANO               --������ˮ��
                ,CONTRACTID                                             AS CONTRACTID           --��ͬID
                ,SCONTRACTID                                            AS SCONTRACTID          --Դ��ͬID
                ,SSYSID                                                 AS SSYSID               --ԴϵͳID
                ,CLIENTID                                               AS CLIENTID             --��������ID
                ,SORGID                                                 AS SORGID               --Դ����ID
                ,SORGNAME                                               AS SORGNAME             --Դ��������
                ,ORGSORTNO                                              AS ORGSORTNO            --�������������
                ,ORGID                                                  AS ORGID                --��������ID
                ,ORGNAME                                                AS ORGNAME              --������������
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --������ҵ����
                ,NVL(INDUSTRYNAME,'δ֪'��                              AS INDUSTRYNAME         --������ҵ����
                ,BUSINESSLINE                                           AS BUSINESSLINE         --����
                ,ASSETTYPE                                              AS ASSETTYPE            --�ʲ�����
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --�ʲ�С��
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --ҵ��Ʒ�ִ���
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --ҵ��Ʒ������
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --���÷�����������
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --��������
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --ԭʼ����
                ,RESIDUALM                                              AS RESIDUALM            --ʣ������
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --�������
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --��ͬ�ܽ��
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --��ͬδ��ȡ����
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --�Ƿ����ʱ����������
                ,ABSUAFLAG                                              AS ABSUAFLAG            --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPOOLID                                              AS ABSPOOLID            --�ʲ�֤ȯ����ID
                ,GROUPID                                                AS GROUPID              --������
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--��Ҫ������ʽ
                ,ABSPROPORTION                                          AS ABSPROPORTION        --�ʲ�֤ȯ������
    FROM 				RWA_DEV.RWA_DZ_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;

     /*����ֱ�����еĺ�ͬ��Ϣ*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
               DATADATE                              --��������
              ,DATANO                                --������ˮ��
              ,CONTRACTID                            --��ͬID
              ,SCONTRACTID                           --Դ��ͬID
              ,SSYSID                                --ԴϵͳID
              ,CLIENTID                              --��������ID
              ,SORGID                                --Դ����ID
              ,SORGNAME                              --Դ��������
              ,ORGSORTNO                             --�������������
              ,ORGID                                 --��������ID
              ,ORGNAME                               --������������
              ,INDUSTRYID                            --������ҵ����
              ,INDUSTRYNAME                          --������ҵ����
              ,BUSINESSLINE                          --����
              ,ASSETTYPE                             --�ʲ�����
              ,ASSETSUBTYPE                          --�ʲ�С��
              ,BUSINESSTYPEID                        --ҵ��Ʒ�ִ���
              ,BUSINESSTYPENAME                      --ҵ��Ʒ������
              ,CREDITRISKDATATYPE                    --���÷�����������
              ,STARTDATE                             --��ʼ����
              ,DUEDATE                               --��������
              ,ORIGINALMATURITY                      --ԭʼ����
              ,RESIDUALM                             --ʣ������
              ,SETTLEMENTCURRENCY                    --�������
              ,CONTRACTAMOUNT                        --��ͬ�ܽ��
              ,NOTEXTRACTPART                        --��ͬδ��ȡ����
              ,UNCONDCANCELFLAG                      --�Ƿ����ʱ����������
              ,ABSUAFLAG                             --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID                             --֤ȯ���ʲ���ID
              ,GROUPID                               --������
              ,GUARANTEETYPE                         --��Ҫ������ʽ
              ,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
     SELECT
                 DATADATE                                               AS DATADATE             --��������
                ,DATANO                                                 AS DATANO               --������ˮ��
                ,CONTRACTID                                             AS CONTRACTID           --��ͬID
                ,SCONTRACTID                                            AS SCONTRACTID          --Դ��ͬID
                ,SSYSID                                                 AS SSYSID               --ԴϵͳID
                ,CLIENTID                                               AS CLIENTID             --��������ID
                ,SORGID                                                 AS SORGID               --Դ����ID
                ,SORGNAME                                               AS SORGNAME             --Դ��������
                ,ORGSORTNO                                              AS ORGSORTNO            --�������������
                ,ORGID                                                  AS ORGID                --��������ID
                ,ORGNAME                                                AS ORGNAME              --������������
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --������ҵ����
                ,NVL(INDUSTRYNAME,'δ֪'��                              AS INDUSTRYNAME         --������ҵ����
                ,BUSINESSLINE                                           AS BUSINESSLINE         --����
                ,ASSETTYPE                                              AS ASSETTYPE            --�ʲ�����
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --�ʲ�С��
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --ҵ��Ʒ�ִ���
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --ҵ��Ʒ������
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --���÷�����������
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --��������
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --ԭʼ����
                ,RESIDUALM                                              AS RESIDUALM            --ʣ������
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --�������
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --��ͬ�ܽ��
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --��ͬδ��ȡ����
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --�Ƿ����ʱ����������
                ,ABSUAFLAG                                              AS ABSUAFLAG            --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPOOLID                                              AS ABSPOOLID            --�ʲ�֤ȯ����ID
                ,GROUPID                                                AS GROUPID              --������
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--��Ҫ������ʽ
                ,ABSPROPORTION                                          AS ABSPROPORTION        --�ʲ�֤ȯ������
    FROM 				RWA_DEV.RWA_ZX_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    
    
     /*��������Ʒҵ��ĺ�ͬ��Ϣ*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
               DATADATE                              --��������
              ,DATANO                                --������ˮ��
              ,CONTRACTID                            --��ͬID
              ,SCONTRACTID                           --Դ��ͬID
              ,SSYSID                                --ԴϵͳID
              ,CLIENTID                              --��������ID
              ,SORGID                                --Դ����ID
              ,SORGNAME                              --Դ��������
              ,ORGSORTNO                             --�������������
              ,ORGID                                 --��������ID
              ,ORGNAME                               --������������
              ,INDUSTRYID                            --������ҵ����
              ,INDUSTRYNAME                          --������ҵ����
              ,BUSINESSLINE                          --����
              ,ASSETTYPE                             --�ʲ�����
              ,ASSETSUBTYPE                          --�ʲ�С��
              ,BUSINESSTYPEID                        --ҵ��Ʒ�ִ���
              ,BUSINESSTYPENAME                      --ҵ��Ʒ������
              ,CREDITRISKDATATYPE                    --���÷�����������
              ,STARTDATE                             --��ʼ����
              ,DUEDATE                               --��������
              ,ORIGINALMATURITY                      --ԭʼ����
              ,RESIDUALM                             --ʣ������
              ,SETTLEMENTCURRENCY                    --�������
              ,CONTRACTAMOUNT                        --��ͬ�ܽ��
              ,NOTEXTRACTPART                        --��ͬδ��ȡ����
              ,UNCONDCANCELFLAG                      --�Ƿ����ʱ����������
              ,ABSUAFLAG                             --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID                             --֤ȯ���ʲ���ID
              ,GROUPID                               --������
              ,GUARANTEETYPE                         --��Ҫ������ʽ
              ,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
     SELECT
                 DATADATE                                               AS DATADATE             --��������
                ,DATANO                                                 AS DATANO               --������ˮ��
                ,CONTRACTID                                             AS CONTRACTID           --��ͬID
                ,SCONTRACTID                                            AS SCONTRACTID          --Դ��ͬID
                ,SSYSID                                                 AS SSYSID               --ԴϵͳID
                ,CLIENTID                                               AS CLIENTID             --��������ID
                ,SORGID                                                 AS SORGID               --Դ����ID
                ,SORGNAME                                               AS SORGNAME             --Դ��������
                ,ORGSORTNO                                              AS ORGSORTNO            --�������������
                ,ORGID                                                  AS ORGID                --��������ID
                ,ORGNAME                                                AS ORGNAME              --������������
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --������ҵ����
                ,NVL(INDUSTRYNAME,'δ֪'��                              AS INDUSTRYNAME         --������ҵ����
                ,BUSINESSLINE                                           AS BUSINESSLINE         --����
                ,ASSETTYPE                                              AS ASSETTYPE            --�ʲ�����
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --�ʲ�С��
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --ҵ��Ʒ�ִ���
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --ҵ��Ʒ������
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --���÷�����������
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --��������
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --ԭʼ����
                ,RESIDUALM                                              AS RESIDUALM            --ʣ������
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --�������
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --��ͬ�ܽ��
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --��ͬδ��ȡ����
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --�Ƿ����ʱ����������
                ,ABSUAFLAG                                              AS ABSUAFLAG            --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPOOLID                                              AS ABSPOOLID            --�ʲ�֤ȯ����ID
                ,GROUPID                                                AS GROUPID              --������
                ,GUARANTEETYPE                                          AS GUARANTEETYPE        --��Ҫ������ʽ
                ,ABSPROPORTION                                          AS ABSPROPORTION        --�ʲ�֤ȯ������
    FROM        RWA_DEV.RWA_YSP_CONTRACT
    WHERE       DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    
    
    /*�������ѽ���ҵ��ĺ�ͬ��Ϣ*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
               DATADATE                              --��������
              ,DATANO                                --������ˮ��
              ,CONTRACTID                            --��ͬID
              ,SCONTRACTID                           --Դ��ͬID
              ,SSYSID                                --ԴϵͳID
              ,CLIENTID                              --��������ID
              ,SORGID                                --Դ����ID
              ,SORGNAME                              --Դ��������
              ,ORGSORTNO                             --�������������
              ,ORGID                                 --��������ID
              ,ORGNAME                               --������������
              ,INDUSTRYID                            --������ҵ����
              ,INDUSTRYNAME                          --������ҵ����
              ,BUSINESSLINE                          --����
              ,ASSETTYPE                             --�ʲ�����
              ,ASSETSUBTYPE                          --�ʲ�С��
              ,BUSINESSTYPEID                        --ҵ��Ʒ�ִ���
              ,BUSINESSTYPENAME                      --ҵ��Ʒ������
              ,CREDITRISKDATATYPE                    --���÷�����������
              ,STARTDATE                             --��ʼ����
              ,DUEDATE                               --��������
              ,ORIGINALMATURITY                      --ԭʼ����
              ,RESIDUALM                             --ʣ������
              ,SETTLEMENTCURRENCY                    --�������
              ,CONTRACTAMOUNT                        --��ͬ�ܽ��
              ,NOTEXTRACTPART                        --��ͬδ��ȡ����
              ,UNCONDCANCELFLAG                      --�Ƿ����ʱ����������
              ,ABSUAFLAG                             --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID                             --֤ȯ���ʲ���ID
              ,GROUPID                               --������
              ,GUARANTEETYPE                         --��Ҫ������ʽ
              ,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
     SELECT
                 DATADATE                                               AS DATADATE             --��������
                ,DATANO                                                 AS DATANO               --������ˮ��
                ,CONTRACTID                                             AS CONTRACTID           --��ͬID
                ,SCONTRACTID                                            AS SCONTRACTID          --Դ��ͬID
                ,SSYSID                                                 AS SSYSID               --ԴϵͳID
                ,CLIENTID                                               AS CLIENTID             --��������ID
                ,SORGID                                                 AS SORGID               --Դ����ID
                ,SORGNAME                                               AS SORGNAME             --Դ��������
                ,ORGSORTNO                                              AS ORGSORTNO            --�������������
                ,ORGID                                                  AS ORGID                --��������ID
                ,ORGNAME                                                AS ORGNAME              --������������
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --������ҵ����
                ,NVL(INDUSTRYNAME,'δ֪'��                              AS INDUSTRYNAME         --������ҵ����
                ,BUSINESSLINE                                           AS BUSINESSLINE         --����
                ,ASSETTYPE                                              AS ASSETTYPE            --�ʲ�����
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --�ʲ�С��
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --ҵ��Ʒ�ִ���
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --ҵ��Ʒ������
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --���÷�����������
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --��������
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --ԭʼ����
                ,RESIDUALM                                              AS RESIDUALM            --ʣ������
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --�������
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --��ͬ�ܽ��
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --��ͬδ��ȡ����
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --�Ƿ����ʱ����������
                ,ABSUAFLAG                                              AS ABSUAFLAG            --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPOOLID                                              AS ABSPOOLID            --�ʲ�֤ȯ����ID
                ,GROUPID                                                AS GROUPID              --������
                ,GUARANTEETYPE                                          AS GUARANTEETYPE        --��Ҫ������ʽ
                ,ABSPROPORTION                                          AS ABSPROPORTION        --�ʲ�֤ȯ������
    FROM        RWA_DEV.RWA_XF_CONTRACT
    WHERE       DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    
    -----------20191120 wzb---
       update rwa_ei_contract
   set ORGID='9998',SORGID='9998',ORGNAME='���⴦��'
   where  ORGID IS NULL AND  datano=P_DATA_DT_STR;
  COMMIT;
    
    --�������Ϣ
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_CONTRACT',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_CONTRACT',partname => 'CONTRACT'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_CONTRACT WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_CONTRACT��ǰ��������ݼ�¼Ϊ:' || v_count1 || '��');

    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'YYYY-MM-DD hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '���÷��պ�ͬ��(RWA_DEV.PRO_RWA_EI_CONTRACT)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_CONTRACT;
/

