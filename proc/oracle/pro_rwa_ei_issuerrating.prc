CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_ISSUERRATING(
														p_data_dt_str IN  VARCHAR2, --��������
                            p_po_rtncode  OUT VARCHAR2, --���ر��
                            p_po_rtnmsg   OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_ISSUERRATING
    ʵ�ֹ���:���ܷ�����������Ϣ��,�������з�����������Ϣ
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2016-07-07
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_LC_ISSUERRATING|���ծȯ������������Ϣ��
    Դ  ��2 :RWA_DEV.RWA_ZQ_ISSUERRATING|ծȯ������������Ϣ��
    Դ  ��3 :RWA_DEV.RWA_ZJ_ISSUERRATING|ծȯ������������Ϣ��-�ʽ�ϵͳ   
    
    Ŀ���1 :RWA_DEV.RWA_EI_ISSUERRATING|������������Ϣ���ܱ�
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    pxl  2018/05/08 �����ʽ�ϵͳծȯ��������Ϣ��Ŀ���
    pxl  2019/09/05  �Ƴ� ��ơ�����ϵͳ���ծȯ ֻ�����ʽ�ϵͳ�е� 11010101 �Թ��ʼ�ֵ��������䶯���뵱������Ľ����ʲ� ծȯ
    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_ISSUERRATING';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_ISSUERRATING DROP PARTITION ISSUERRATING' || p_data_dt_str;

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
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_ISSUERRATING ADD PARTITION ISSUERRATING' || p_data_dt_str || ' VALUES(TO_DATE(' || p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;


    --DBMS_OUTPUT.PUT_LINE('��ʼ�����롾������������Ϣ��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

		/*�������ծȯ�ķ�����������Ϣ
    INSERT INTO RWA_DEV.RWA_EI_ISSUERRATING(
        				DATADATE                               --��������
                ,ISSUERID                           	 --������ID
                ,ISSUERNAME                    	 	 		 --����������
                ,RATINGORG                     	 	 		 --��������
                ,RATINGRESULT                  	 	 		 --�������
                ,RATINGDATE                    	 	 		 --��������
                ,FETCHFLAG                     	 	 		 --ȡ����ʶ
    )
    SELECT
    						DATADATE						                         AS DATADATE
        				,ISSUERID                         					 AS ISSUERID
        				,ISSUERNAME                									 AS ISSUERNAME
        				,RATINGORG               										 AS RATINGORG
        				,RATINGRESULT               								 AS RATINGRESULT
        				,TO_CHAR(TO_DATE(RATINGDATE,'YYYYMMDD'),'YYYY-MM-DD')
        										                             	   AS RATINGDATE
        				,FETCHFLAG                             			 AS FETCHFLAG
    FROM 				RWA_DEV.RWA_LC_ISSUERRATING
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
   	;

		COMMIT;

    */
    
		/*����ծȯ�ķ�����������Ϣ
    INSERT INTO RWA_DEV.RWA_EI_ISSUERRATING(
        				DATADATE                               --��������
                ,ISSUERID                           	 --������ID
                ,ISSUERNAME                    	 	 		 --����������
                ,RATINGORG                     	 	 		 --��������
                ,RATINGRESULT                  	 	 		 --�������
                ,RATINGDATE                    	 	 		 --��������
                ,FETCHFLAG                     	 	 		 --ȡ����ʶ
    )
    SELECT
    						T1.DATADATE						                       AS DATADATE
        				,T1.ISSUERID                         				 AS ISSUERID
        				,T1.ISSUERNAME                							 AS ISSUERNAME
        				,T1.RATINGORG               								 AS RATINGORG
        				,T1.RATINGRESULT               							 AS RATINGRESULT
        				,TO_CHAR(TO_DATE(T1.RATINGDATE,'YYYYMMDD'),'YYYY-MM-DD')
        											                            	 AS RATINGDATE
        				,T1.FETCHFLAG                             	 AS FETCHFLAG
    FROM 				RWA_DEV.RWA_ZQ_ISSUERRATING T1
    WHERE				NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_ISSUERRATING T2 WHERE T1.ISSUERID = T2.ISSUERID AND T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD'))
    AND					T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
   	;

		COMMIT;
    
    */
    
      /*����ծȯ�ķ�����������Ϣ-�ʽ�ϵͳ*/
    INSERT INTO RWA_DEV.RWA_EI_ISSUERRATING(
                DATADATE                               --��������
                ,ISSUERID                              --������ID
                ,ISSUERNAME                            --����������
                ,RATINGORG                             --��������
                ,RATINGRESULT                          --�������
                ,RATINGDATE                            --��������
                ,FETCHFLAG                             --ȡ����ʶ
    )
    SELECT
                T1.DATADATE                                  AS DATADATE
                ,T1.ISSUERID                                 AS ISSUERID
                ,T1.ISSUERNAME                               AS ISSUERNAME
                ,T1.RATINGORG                                AS RATINGORG
                ,T1.RATINGRESULT                             AS RATINGRESULT
                ,TO_CHAR(TO_DATE(T1.RATINGDATE,'YYYYMMDD'),'YYYY-MM-DD')
                                                             AS RATINGDATE
                ,T1.FETCHFLAG                                AS FETCHFLAG
    FROM        RWA_DEV.RWA_ZJ_ISSUERRATING T1 --ծȯ������������Ϣ��-�ʽ�ϵͳ   
    WHERE       T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;

    COMMIT;

    

		--�������Ϣ
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_ISSUERRATING',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_ISSUERRATING',partname => 'ISSUERRATING'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    --DBMS_OUTPUT.PUT_LINE('���������롾������������Ϣ��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_ISSUERRATING WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_ISSUERRATING��ǰ���ܷ�����������Ϣ���ݼ�¼Ϊ: ' || v_count || ' ��');

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count;

    --�����쳣
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '������������Ϣ����('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_EI_ISSUERRATING;
/

