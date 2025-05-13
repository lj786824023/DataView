CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_SMZ_GUARANTEE(
			 											p_data_dt_str	IN	VARCHAR2,		--��������
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:PRO_RWA_SMZ_GUARANTEE
    ʵ�ֹ���:˽ļծ-��֤,��ṹΪ��֤��
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2015-06-29
    ��  λ	:�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1	:RWA_WS_PRIVATE_BOND|˽ļծҵ��¼ģ��
    Ŀ���	:RWA_SMZ_GUARANTEE|˽ļծ-��֤��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
  */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'PRO_RWA_SMZ_GUARANTEE';
  --�����ж�ֵ����
  v_count INTEGER;
  --�����쳣����
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_SMZ_GUARANTEE';

    --����Ч����¶�Ӧ��ͬ�ı�֤���뵽Ŀ�����
    INSERT INTO RWA_SMZ_GUARANTEE(
         				 DATADATE          												  --��������
								,DATANO                                     --������ˮ��
								,GUARANTEEID                                --��֤ID
								,SSYSID                                     --ԴϵͳID
								,GUARANTEECONID                             --��֤��ͬID
								,GUARANTORID                                --��֤��ID
								,CREDITRISKDATATYPE                         --���÷�����������
								,GUARANTEEWAY                            		--������ʽ
								,QUALFLAGSTD                            		--Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB                               --�����������ϸ��ʶ
								,GUARANTEETYPESTD                           --Ȩ�ط���֤����
								,GUARANTORSDVSSTD                           --Ȩ�ط���֤��ϸ��
								,GUARANTEETYPEIRB                           --��������֤����
								,GUARANTEEAMOUNT                            --��֤�ܶ�
								,CURRENCY                                   --����
								,STARTDATE                                  --��ʼ����
								,DUEDATE                                    --��������
								,ORIGINALMATURITY                           --ԭʼ����
								,RESIDUALM                                  --ʣ������
								,GUARANTORIRATING                           --��֤���ڲ�����
								,GUARANTORPD                                --��֤��ΥԼ����
								,GROUPID                                    --������
    )
    SELECT
         				 TO_DATE(p_data_dt_str,'YYYYMMDD')							  AS  DATADATE          	 --��������
         				,p_data_dt_str																 		AS	DATANO               --������ˮ��
         				,T1.DBID		                                      AS	GUARANTEEID          --��֤ID
								,'SMZ'																						AS	SSYSID               --ԴϵͳID
								,T1.DBID																		      AS	GUARANTEECONID       --��֤��ͬID
								,T1.CUSTID2																		    AS	GUARANTORID          --��֤��ID
								,'06'																							AS	CREDITRISKDATATYPE   --���÷�����������
								,T1.DBLX																	        AS	GUARANTEEWAY       	 --������ʽ
								,''																								AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,''																								AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,''																								AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,''																								AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,''																								AS	GUARANTEETYPEIRB     --��������֤����
								,ROUND(TO_NUMBER(T1.DBJZ),6)							        AS	GUARANTEEAMOUNT      --��֤�ܶ�
								,NVL(T1.DBBZDM,'CNY')										  				AS	CURRENCY             --����
								,REPLACE(T1.DBQSR,'-','')													AS	STARTDATE            --��ʼ����
								,REPLACE(T1.DBDQR,'-','')													AS	DUEDATE              --��������
								,CASE WHEN (to_date(T1.DBDQR,'yyyy-mm-dd') - to_date(T1.DBQSR,'yyyy-mm-dd')) / 365<0
                      THEN 0
                      ELSE (to_date(T1.DBDQR,'yyyy-mm-dd') - to_date(T1.DBQSR,'yyyy-mm-dd')) / 365
                END																					      AS ORIGINALMATURITY				 --ԭʼ����									��λ����
								,CASE WHEN (to_date(T1.DBDQR,'yyyy-mm-dd') - to_date(p_data_dt_str,'yyyymmdd')) / 365<0
								      THEN 0
								      ELSE (to_date(T1.DBDQR,'yyyy-mm-dd') - to_date(p_data_dt_str,'yyyymmdd')) / 365
								END																					      AS  RESIDUALM								 --ʣ������
								,''																								AS	GUARANTORIRATING     --��֤���ڲ�����
								,''																								AS	GUARANTORPD          --��֤��ΥԼ����
								,''																								AS	GROUPID              --������
    FROM  RWA.RWA_WS_PRIVATE_BOND T1
    INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT T2                    --���ݲ�¼��
    ON          T1.SUPPORGID=T2.ORGID
    AND         T2.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T2.SUPPTMPLID='M-0110'
    AND         T2.SUBMITFLAG='1'
    WHERE T1.DBLX IN ('030010','030020','030030','020080','020090')
    AND   T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_SMZ_GUARANTEE;
    --Dbms_output.Put_line('RWA_SMZ_GUARANTEE��ǰ��������ݼ�¼Ϊ:' || v_count1 || '��');

		--Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

		p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '˽ļծ-��֤(PRO_RWA_SMZ_GUARANTEE)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_SMZ_GUARANTEE;
/

