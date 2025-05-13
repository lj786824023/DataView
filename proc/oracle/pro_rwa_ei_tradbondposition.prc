CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_TRADBONDPOSITION(
														p_data_dt_str IN  VARCHAR2, --��������
                            p_po_rtncode  OUT VARCHAR2, --���ر��
                            p_po_rtnmsg   OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_TRADBONDPOSITION
    ʵ�ֹ���:���ܽ���ծȯͷ����������н���ծȯͷ����Ϣ
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2016-07-07
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_LC_TRADBONDPOSITION|���ծȯ����ծȯͷ���
    Դ  ��2 :RWA_DEV.RWA_ZQ_TRADBONDPOSITION|ծȯ����ծȯͷ���
    Դ  ��3 :RWA_DEV.RWA_ZJ_TRADBONDPOSITION|ծȯ����ծȯͷ��� -�ʽ�ϵͳ   
    
    Ŀ���1 :RWA_DEV.RWA_EI_TRADBONDPOSITION|����ծȯͷ����ܱ�
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    pxl 2019/05/08  �����ʽ�ϵͳծȯͷ����Ϣ   
    pxl  2019/09/05  �Ƴ� ��ơ�����ϵͳ���ծȯ ֻ�����ʽ�ϵͳ�е� 11010101 �Թ��ʼ�ֵ��������䶯���뵱������Ľ����ʲ� ծȯ
    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_TRADBONDPOSITION';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_TRADBONDPOSITION DROP PARTITION TRADBONDPOSITION' || p_data_dt_str;

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
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_TRADBONDPOSITION ADD PARTITION TRADBONDPOSITION' || p_data_dt_str || ' VALUES(TO_DATE(' || p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;


    --DBMS_OUTPUT.PUT_LINE('��ʼ�����롾����ծȯͷ���' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�������ծȯ�Ľ���ծȯͷ����Ϣ
    INSERT INTO RWA_DEV.RWA_EI_TRADBONDPOSITION(
        				DATADATE                               --��������
                ,POSITIONID                          	 --ͷ��ID
                ,BONDID                              	 --ծȯID
                ,TRANORGID                           	 --���׻���ID
                ,ACCORGID                            	 --�������ID
                ,INSTRUMENTSTYPE                     	 --���ڹ�������
                ,ACCSUBJECTS                         	 --��ƿ�Ŀ
                ,DENOMINATION                        	 --���
                ,MARKETVALUE                         	 --��ֵ
                ,DISCOUNTPREMIUM                     	 --�����
                ,FAIRVALUECHANGE                     	 --���ʼ�ֵ�䶯
                ,BOOKBALANCE                         	 --�������
                ,CURRENCY                            	 --����
                ,SYSID                                 --ϵͳ��Դ���ݲ�����
    )
    SELECT
        				DATADATE      				                        AS DATADATE
        				,POSITIONID			                              AS POSITIONID
        				,BONDID																        AS BONDID
        				,TRANORGID                          					AS TRANORGID
        				,ACCORGID                          					  AS ACCORGID
        				,INSTRUMENTSTYPE                              AS INSTRUMENTSTYPE
        				,ACCSUBJECTS                                  AS ACCSUBJECTS
        				,DENOMINATION                 					      AS DENOMINATION
        				,MARKETVALUE                   					      AS MARKETVALUE
        				,DISCOUNTPREMIUM                              AS DISCOUNTPREMIUM
        				,FAIRVALUECHANGE                              AS FAIRVALUECHANGE
        				,BOOKBALANCE                					        AS BOOKBALANCE
        				,CURRENCY                    					        AS CURRENCY
                ,'LC'

    FROM				RWA_DEV.RWA_LC_TRADBONDPOSITION
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;

		COMMIT;

    */

		/*����ծȯ�Ľ���ծȯͷ����Ϣ
    INSERT INTO RWA_DEV.RWA_EI_TRADBONDPOSITION(
        				DATADATE                               --��������
                ,POSITIONID                          	 --ͷ��ID
                ,BONDID                              	 --ծȯID
                ,TRANORGID                           	 --���׻���ID
                ,ACCORGID                            	 --�������ID
                ,INSTRUMENTSTYPE                     	 --���ڹ�������
                ,ACCSUBJECTS                         	 --��ƿ�Ŀ
                ,DENOMINATION                        	 --���
                ,MARKETVALUE                         	 --��ֵ
                ,DISCOUNTPREMIUM                     	 --�����
                ,FAIRVALUECHANGE                     	 --���ʼ�ֵ�䶯
                ,BOOKBALANCE                         	 --�������
                ,CURRENCY                            	 --����
                ,SYSID                                 --ϵͳ��Դ���ݲ�����
    )
    SELECT
        				DATADATE      				                        AS DATADATE
        				,POSITIONID			                              AS POSITIONID
        				,BONDID																        AS BONDID
        				,TRANORGID                          					AS TRANORGID
        				,ACCORGID                          					  AS ACCORGID
        				,INSTRUMENTSTYPE                              AS INSTRUMENTSTYPE
        				,ACCSUBJECTS                                  AS ACCSUBJECTS
        				,DENOMINATION                 					      AS DENOMINATION
        				,MARKETVALUE                   					      AS MARKETVALUE
        				,DISCOUNTPREMIUM                              AS DISCOUNTPREMIUM
        				,FAIRVALUECHANGE                              AS FAIRVALUECHANGE
        				,BOOKBALANCE                					        AS BOOKBALANCE
        				,CURRENCY                    					        AS CURRENCY
                ,'FS'                                 --ϵͳ��Դ���ݲ�����

    FROM				RWA_DEV.RWA_ZQ_TRADBONDPOSITION
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;

		COMMIT;
    
    */
    
    
    /*����ծȯ�Ľ���ծȯͷ����Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_TRADBONDPOSITION(
                DATADATE                               --��������
                ,POSITIONID                            --ͷ��ID
                ,BONDID                                --ծȯID
                ,TRANORGID                             --���׻���ID
                ,ACCORGID                              --�������ID
                ,INSTRUMENTSTYPE                       --���ڹ�������
                ,ACCSUBJECTS                           --��ƿ�Ŀ
                ,DENOMINATION                          --���
                ,MARKETVALUE                           --��ֵ
                ,DISCOUNTPREMIUM                       --�����
                ,FAIRVALUECHANGE                       --���ʼ�ֵ�䶯
                ,BOOKBALANCE                           --�������
                ,CURRENCY                              --����
                ,SYSID                                 --ϵͳ��Դ���ݲ�����
    )
    SELECT
                DATADATE                                      AS DATADATE
                ,POSITIONID                                   AS POSITIONID
                ,BONDID                                       AS BONDID
                ,TRANORGID                                    AS TRANORGID
                ,ACCORGID                                     AS ACCORGID
                ,INSTRUMENTSTYPE                              AS INSTRUMENTSTYPE
                ,ACCSUBJECTS                                  AS ACCSUBJECTS
                ,DENOMINATION                                 AS DENOMINATION
                ,MARKETVALUE                                  AS MARKETVALUE
                ,DISCOUNTPREMIUM                              AS DISCOUNTPREMIUM
                ,FAIRVALUECHANGE                              AS FAIRVALUECHANGE
                ,BOOKBALANCE                                  AS BOOKBALANCE
                ,CURRENCY                                     AS CURRENCY
                ,'ZJ'

    FROM        RWA_DEV.RWA_ZJ_TRADBONDPOSITION   --ծȯ����ծȯͷ��� -�ʽ�ϵͳ
    WHERE       DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;

    COMMIT;
    
    

		--�������Ϣ
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_TRADBONDPOSITION',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_TRADBONDPOSITION',partname => 'TRADBONDPOSITION'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    --DBMS_OUTPUT.PUT_LINE('���������롾����ծȯͷ���' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_TRADBONDPOSITION WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_TRADBONDPOSITION��ǰ���ܽ���ծȯͷ���¼Ϊ: ' || v_count || ' ��');

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count;


    --�����쳣
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '����ծȯͷ�����('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_EI_TRADBONDPOSITION;
/

