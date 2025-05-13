CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_BONDINFO(
														p_data_dt_str  IN  VARCHAR2, --��������
                            p_po_rtncode   OUT VARCHAR2, --���ر��
                            p_po_rtnmsg    OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_BONDINFO
    ʵ�ֹ���:����ծȯ��Ϣ����������ծȯ��Ϣ
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2016-07-07
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_LC_BONDINFO|���ծȯ��ծȯ��Ϣ��
    Դ  ��2 :RWA_DEV.RWA_ZQ_BONDINFO|ծȯ��ծȯ��Ϣ��
    Դ  ��3 :RWA_DEV.RWA_ZJ_BONDINFO|ծȯ��Ϣ��-�ʽ�ϵͳ
    Դ  ��4 :RWA_DEV.RWA_EI_CLIENT|�ͻ����ܱ�
    Ŀ���1 :RWA_DEV.RWA_EI_BONDINFO|����ծȯ��Ϣ��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    pxl  2019/05/08  �����ʽ�ϵͳծȯ��Ϣ����
    pxl  2019/09/05  �Ƴ� ��ơ�����ϵͳ���ծȯ ֻ�����ʽ�ϵͳ�е� 11010101 �Թ��ʼ�ֵ��������䶯���뵱������Ľ����ʲ� ծȯ
   */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_BONDINFO';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_BONDINFO DROP PARTITION BONDINFO' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --�״η���truncate�����2149�쳣
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '����ծȯ��Ϣ��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --����һ����ǰ�����µķ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_BONDINFO ADD PARTITION BONDINFO' || p_data_dt_str || ' VALUES(TO_DATE(' || p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;


    --DBMS_OUTPUT.PUT_LINE('��ʼ�����롾ծȯ��Ϣ��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�������ծȯ��ծȯ��Ϣ
    INSERT INTO RWA_DEV.RWA_EI_BONDINFO(
        				DATADATE                               --��������
                ,BONDID                                --ծȯID
                ,BONDNAME                              --ծȯ����
                ,BONDTYPE                              --ծȯ����
                ,ERATING                               --�ⲿ����
                ,ISSUERID                              --������ID
                ,ISSUERNAME                            --����������
                ,ISSUERTYPE                            --�����˴���
                ,ISSUERSUBTYPE                         --������С��
                ,ISSUERREGISTSTATE                     --������ע�����
                ,ISSUERSMBFLAG                         --������С΢��ҵ��ʶ
                ,BONDISSUEINTENT                       --ծȯ����Ŀ��
                ,REABSFLAG                             --���ʲ�֤ȯ����ʶ
                ,ORIGINATORFLAG                        --�Ƿ������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                      --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,RATETYPE                              --��������
                ,EXECUTIONRATE                         --ִ������
                ,NEXTREPRICEDATE                       --�´��ض�����
                ,NEXTREPRICEM                          --�´��ض�������
                ,MODIFIEDDURATION                      --��������
                ,DENOMINATION                          --���
                ,CURRENCY                              --����
    )
    SELECT
        				DATADATE                                     AS DATADATE                 --��������
                ,BONDID												     	         AS BONDID                   --ծȯID
                ,BONDNAME                  				 		       AS BONDNAME                 --ծȯ����
                ,BONDTYPE                          		       AS BONDTYPE                 --ծȯ����
                ,ERATING                                     AS ERATING                  --�ⲿ����          					 ��¼��ͨ�����ޡ��������ȼ�ת��Ϊ����
                ,ISSUERID                           			   AS ISSUERID                 --������ID          					 ��¼
                ,ISSUERNAME					                         AS ISSUERNAME               --����������        					 ��¼
                ,ISSUERTYPE			 											       AS ISSUERTYPE               --�����˴���        					 ����ӳ��
                ,ISSUERSUBTYPE							                 AS ISSUERSUBTYPE            --������С��        					  ����ӳ��
                ,ISSUERREGISTSTATE							             AS ISSUERREGISTSTATE        --������ע�����    					 	Ĭ�ϣ��й�
                ,ISSUERSMBFLAG								               AS ISSUERSMBFLAG            --������С΢��ҵ��ʶ					 	Ĭ�ϣ���(0)
                ,BONDISSUEINTENT											       AS BONDISSUEINTENT          --ծȯ����Ŀ��      					 Ĭ�ϣ�����(02)
                ,REABSFLAG						                       AS REABSFLAG                --���ʲ�֤ȯ����ʶ  					 	Ĭ�ϣ���(0)
                ,ORIGINATORFLAG															 AS ORIGINATORFLAG   				 --�Ƿ������      					 1. ���������ƣ��������У���Ϊ�ǣ� 2. ����Ϊ��
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS STARTDATE                --��ʼ����          					 ��¼
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                	                                           AS DUEDATE                  --��������
                ,ORIGINALMATURITY                            AS ORIGINALMATURITY         --ԭʼ����
                ,RESIDUALM                                   AS RESIDUALM                --ʣ������
                ,RATETYPE                                    AS RATETYPE                 --��������
                ,EXECUTIONRATE                               AS EXECUTIONRATE            --ִ������
                ,NEXTREPRICEDATE                             AS NEXTREPRICEDATE          --�´��ض�����      					1. ���������ͣ��̶������´��ض����գ��������ڣ�2. ����ȡϵͳ�ֶ� ��¼
                ,NEXTREPRICEM                                AS NEXTREPRICEM             --�´��ض�������    					1. ���������ͣ��̶�����Ĭ��Ϊ��NULL��2. ����ȡϵͳ�ֶ�
                ,MODIFIEDDURATION                            AS MODIFIEDDURATION         --��������
                ,DENOMINATION                                AS DENOMINATION             --���
                ,CURRENCY 	                                 AS CURRENCY                 --����

   	FROM				RWA_DEV.RWA_LC_BONDINFO
   	WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
 		;

 		COMMIT;
*/

 		/*����ծȯ��ծȯ��Ϣ
    INSERT INTO RWA_DEV.RWA_EI_BONDINFO(
        				DATADATE                               --��������
                ,BONDID                                --ծȯID
                ,BONDNAME                              --ծȯ����
                ,BONDTYPE                              --ծȯ����
                ,ERATING                               --�ⲿ����
                ,ISSUERID                              --������ID
                ,ISSUERNAME                            --����������
                ,ISSUERTYPE                            --�����˴���
                ,ISSUERSUBTYPE                         --������С��
                ,ISSUERREGISTSTATE                     --������ע�����
                ,ISSUERSMBFLAG                         --������С΢��ҵ��ʶ
                ,BONDISSUEINTENT                       --ծȯ����Ŀ��
                ,REABSFLAG                             --���ʲ�֤ȯ����ʶ
                ,ORIGINATORFLAG                        --�Ƿ������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                      --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,RATETYPE                              --��������
                ,EXECUTIONRATE                         --ִ������
                ,NEXTREPRICEDATE                       --�´��ض�����
                ,NEXTREPRICEM                          --�´��ض�������
                ,MODIFIEDDURATION                      --��������
                ,DENOMINATION                          --���
                ,CURRENCY                              --����
    )
    SELECT
        				DATADATE                                     AS DATADATE                 --��������
                ,BONDID												     	         AS BONDID                   --ծȯID
                ,BONDNAME                  				 		       AS BONDNAME                 --ծȯ����
                ,BONDTYPE                          		       AS BONDTYPE                 --ծȯ����
                ,ERATING                                     AS ERATING                  --�ⲿ����          					 ��¼��ͨ�����ޡ��������ȼ�ת��Ϊ����
                ,ISSUERID                           			   AS ISSUERID                 --������ID          					 ��¼
                ,ISSUERNAME					                         AS ISSUERNAME               --����������        					 ��¼
                ,ISSUERTYPE			 											       AS ISSUERTYPE               --�����˴���        					 ����ӳ��
                ,ISSUERSUBTYPE							                 AS ISSUERSUBTYPE            --������С��        					  ����ӳ��
                ,ISSUERREGISTSTATE							             AS ISSUERREGISTSTATE        --������ע�����    					 	Ĭ�ϣ��й�
                ,ISSUERSMBFLAG								               AS ISSUERSMBFLAG            --������С΢��ҵ��ʶ					 	Ĭ�ϣ���(0)
                ,BONDISSUEINTENT											       AS BONDISSUEINTENT          --ծȯ����Ŀ��      					 Ĭ�ϣ�����(02)
                ,REABSFLAG						                       AS REABSFLAG                --���ʲ�֤ȯ����ʶ  					 	Ĭ�ϣ���(0)
                ,ORIGINATORFLAG															 AS ORIGINATORFLAG   				 --�Ƿ������      					 1. ���������ƣ��������У���Ϊ�ǣ� 2. ����Ϊ��
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS STARTDATE                --��ʼ����          					 ��¼
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                	                                           AS DUEDATE                  --��������
                ,ORIGINALMATURITY                            AS ORIGINALMATURITY         --ԭʼ����
                ,RESIDUALM                                   AS RESIDUALM                --ʣ������
                ,RATETYPE                                    AS RATETYPE                 --��������
                ,EXECUTIONRATE                               AS EXECUTIONRATE            --ִ������
                ,NEXTREPRICEDATE                             AS NEXTREPRICEDATE          --�´��ض�����      					1. ���������ͣ��̶������´��ض����գ��������ڣ�2. ����ȡϵͳ�ֶ� ��¼
                ,NEXTREPRICEM                                AS NEXTREPRICEM             --�´��ض�������    					1. ���������ͣ��̶�����Ĭ��Ϊ��NULL��2. ����ȡϵͳ�ֶ�
                ,MODIFIEDDURATION                            AS MODIFIEDDURATION         --��������
                ,DENOMINATION                                AS DENOMINATION             --���
                ,CURRENCY 	                                 AS CURRENCY                 --����

   	FROM				RWA_DEV.RWA_ZQ_BONDINFO
   	WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
 		;

 		COMMIT;
    
    */
    
        
    /*�����ʽ�ϵͳծȯ��Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_BONDINFO(
                DATADATE                               --��������
                ,BONDID                                --ծȯID
                ,BONDNAME                              --ծȯ����
                ,BONDTYPE                              --ծȯ����
                ,ERATING                               --�ⲿ����
                ,ISSUERID                              --������ID
                ,ISSUERNAME                            --����������
                ,ISSUERTYPE                            --�����˴���
                ,ISSUERSUBTYPE                         --������С��
                ,ISSUERREGISTSTATE                     --������ע�����
                ,ISSUERSMBFLAG                         --������С΢��ҵ��ʶ
                ,BONDISSUEINTENT                       --ծȯ����Ŀ��
                ,REABSFLAG                             --���ʲ�֤ȯ����ʶ
                ,ORIGINATORFLAG                        --�Ƿ������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                      --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,RATETYPE                              --��������
                ,EXECUTIONRATE                         --ִ������
                ,NEXTREPRICEDATE                       --�´��ض�����
                ,NEXTREPRICEM                          --�´��ض�������
                ,MODIFIEDDURATION                      --��������
                ,DENOMINATION                          --���
                ,CURRENCY                              --����
    )
    SELECT
                DATADATE                                     AS DATADATE                 --��������
                ,BONDID                                      AS BONDID                   --ծȯID
                ,BONDNAME                                    AS BONDNAME                 --ծȯ����
                ,BONDTYPE                                    AS BONDTYPE                 --ծȯ����
                ,ERATING                                     AS ERATING                  --�ⲿ����                    ��¼��ͨ�����ޡ��������ȼ�ת��Ϊ����
                ,ISSUERID                                    AS ISSUERID                 --������ID                     ��¼
                ,ISSUERNAME                                  AS ISSUERNAME               --����������                   ��¼
                ,ISSUERTYPE                                  AS ISSUERTYPE               --�����˴���                   ����ӳ��
                ,ISSUERSUBTYPE                               AS ISSUERSUBTYPE            --������С��                    ����ӳ��
                ,ISSUERREGISTSTATE                           AS ISSUERREGISTSTATE        --������ע�����                Ĭ�ϣ��й�
                ,ISSUERSMBFLAG                               AS ISSUERSMBFLAG            --������С΢��ҵ��ʶ            Ĭ�ϣ���(0)
                ,BONDISSUEINTENT                             AS BONDISSUEINTENT          --ծȯ����Ŀ��                Ĭ�ϣ�����(02)
                ,REABSFLAG                                   AS REABSFLAG                --���ʲ�֤ȯ����ʶ             Ĭ�ϣ���(0)
                ,ORIGINATORFLAG                              AS ORIGINATORFLAG           --�Ƿ������                1. ���������ƣ��������У���Ϊ�ǣ� 2. ����Ϊ��
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS STARTDATE                --��ʼ����                    ��¼
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS DUEDATE                  --��������
                ,ORIGINALMATURITY                            AS ORIGINALMATURITY         --ԭʼ����
                ,RESIDUALM                                   AS RESIDUALM                --ʣ������
                ,RATETYPE                                    AS RATETYPE                 --��������
                ,EXECUTIONRATE                               AS EXECUTIONRATE            --ִ������
                ,NEXTREPRICEDATE                             AS NEXTREPRICEDATE          --�´��ض�����               1. ���������ͣ��̶������´��ض����գ��������ڣ�2. ����ȡϵͳ�ֶ� ��¼
                ,NEXTREPRICEM                                AS NEXTREPRICEM             --�´��ض�������              1. ���������ͣ��̶�����Ĭ��Ϊ��NULL��2. ����ȡϵͳ�ֶ�
                ,MODIFIEDDURATION                            AS MODIFIEDDURATION         --��������
                ,DENOMINATION                                AS DENOMINATION             --���
                ,CURRENCY                                    AS CURRENCY                 --����

    FROM        RWA_DEV.RWA_ZJ_BONDINFO --ծȯ��Ϣ-�ʽ�ϵͳ
    WHERE       DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;

    COMMIT;

 		----------------------------------------------����ծȯ��Ϣ���ܱ����˴�С��---------------------------------------------------------
    UPDATE RWA_DEV.RWA_EI_BONDINFO T1
      SET T1.ISSUERTYPE = (
                           SELECT T2.CLIENTTYPE
                           FROM RWA_DEV.RWA_EI_CLIENT T2
                           WHERE T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                           AND T1.ISSUERID=T2.CLIENTID
                          )
          ,T1.ISSUERSUBTYPE = (
                               SELECT T2.CLIENTSUBTYPE
                               FROM RWA_DEV.RWA_EI_CLIENT T2
                               WHERE T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                               AND T1.ISSUERID=T2.CLIENTID
                              )
    WHERE   T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND   T1.ISSUERTYPE    IS NULL
      AND   T1.ISSUERSUBTYPE IS NULL
    ;
    COMMIT;

    --�������Ϣ
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_BONDINFO',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_BONDINFO',partname => 'BONDINFO'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    --DBMS_OUTPUT.PUT_LINE('���������롾ծȯ��Ϣ��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_BONDINFO WHERE DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_BONDINFO��ǰ����ծȯ��Ϣ��¼Ϊ: ' || v_count || ' ��');



    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count;

    --�����쳣
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := 'ծȯ��Ϣ����('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_EI_BONDINFO;
/

