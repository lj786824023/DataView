CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_GUARANTEE(
			 											p_data_dt_str	IN	VARCHAR2,		--��������
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_GUARANTEE
    ʵ�ֹ���:���ܱ�֤��,�������б�֤��Ϣ
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2015-06-01
    ��  λ	:�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1	:RWA_DEV.RWA_XD_GUARANTEE|�Ŵ���֤��
    Դ  ��2	:RWA_DEV.RWA_LC_GUARANTEE|��Ʊ�֤��
    Դ  ��3	:RWA_DEV.RWA_TZ_GUARANTEE|Ͷ�ʱ�֤��
    Դ  ��4	:RWA_DEV.RWA_ABS_ISSURE_GUARANTEE|�ʲ�֤ȯ�����л�����֤��
    Ŀ���1	:RWA_DEV.RWA_EI_GUARANTEE|���ܱ�֤��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
  */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_XD_GUARANTEE';
  --�����ж�ֵ����
  v_count INTEGER;
  --�����쳣����
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_GUARANTEE DROP PARTITION GUARANTEE' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --�״η���truncate�����2149�쳣
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '���ܱ�֤��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --����һ����ǰ�����µķ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_GUARANTEE ADD PARTITION GUARANTEE' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*�����Ŵ��ı�֤��Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_GUARANTEE(
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
         				 DATADATE          										AS  DATADATE          		 --��������
         				,DATANO            						 		    AS	DATANO               --������ˮ��
         				,GUARANTEEID       										AS	GUARANTEEID          --��֤ID
								,SSYSID            									  AS	SSYSID               --ԴϵͳID
								,GUARANTEECONID    										AS	GUARANTEECONID       --��֤��ͬID
								,GUARANTORID       										AS	GUARANTORID          --��֤��ID
								,CREDITRISKDATATYPE								    AS	CREDITRISKDATATYPE   --���÷�����������
								,GUARANTEEWAY      										AS	GUARANTEEWAY       	 --������ʽ
								,QUALFLAGSTD       										AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB      										AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,GUARANTEETYPESTD  										AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,GUARANTORSDVSSTD  										AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,GUARANTEETYPEIRB  										AS	GUARANTEETYPEIRB     --��������֤����
								,GUARANTEEAMOUNT   										AS	GUARANTEEAMOUNT      --��֤�ܶ�
								,CURRENCY          					  				AS	CURRENCY             --����
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
								         										          AS	STARTDATE            --��ʼ����
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
								           										        AS	DUEDATE              --��������
								,ORIGINALMATURITY                     AS  ORIGINALMATURITY   	 --ԭʼ����
								,RESIDUALM                            AS	RESIDUALM            --ʣ������
								,GUARANTORIRATING  									  AS	GUARANTORIRATING     --��֤���ڲ�����
								,GUARANTORPD       									  AS	GUARANTORPD          --��֤��ΥԼ����
								,GROUPID           										AS	GROUPID              --������
    FROM 				RWA_DEV.RWA_XD_GUARANTEE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*������Ƶı�֤��Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_GUARANTEE(
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
         				 DATADATE          										AS  DATADATE          		 --��������
         				,DATANO            						 		    AS	DATANO               --������ˮ��
         				,GUARANTEEID       										AS	GUARANTEEID          --��֤ID
								,SSYSID            									  AS	SSYSID               --ԴϵͳID
								,GUARANTEECONID    										AS	GUARANTEECONID       --��֤��ͬID
								,GUARANTORID       										AS	GUARANTORID          --��֤��ID
								,CREDITRISKDATATYPE								    AS	CREDITRISKDATATYPE   --���÷�����������
								,GUARANTEEWAY      										AS	GUARANTEEWAY       	 --������ʽ
								,QUALFLAGSTD       										AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB      										AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,GUARANTEETYPESTD  										AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,GUARANTORSDVSSTD  										AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,GUARANTEETYPEIRB  										AS	GUARANTEETYPEIRB     --��������֤����
								,GUARANTEEAMOUNT   										AS	GUARANTEEAMOUNT      --��֤�ܶ�
								,CURRENCY          					  				AS	CURRENCY             --����
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
								         										          AS	STARTDATE            --��ʼ����
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
								           										        AS	DUEDATE              --��������
								,ORIGINALMATURITY                     AS  ORIGINALMATURITY   	 --ԭʼ����
								,RESIDUALM                            AS	RESIDUALM            --ʣ������
								,GUARANTORIRATING  									  AS	GUARANTORIRATING     --��֤���ڲ�����
								,GUARANTORPD       									  AS	GUARANTORPD          --��֤��ΥԼ����
								,GROUPID           										AS	GROUPID              --������
    FROM 				RWA_DEV.RWA_LC_GUARANTEE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*����Ͷ�ʵı�֤��Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_GUARANTEE(
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
         				 DATADATE          										AS  DATADATE          		 --��������
         				,DATANO            						 		    AS	DATANO               --������ˮ��
         				,GUARANTEEID       										AS	GUARANTEEID          --��֤ID
								,SSYSID            									  AS	SSYSID               --ԴϵͳID
								,GUARANTEECONID    										AS	GUARANTEECONID       --��֤��ͬID
								,GUARANTORID       										AS	GUARANTORID          --��֤��ID
								,CREDITRISKDATATYPE								    AS	CREDITRISKDATATYPE   --���÷�����������
								,GUARANTEEWAY      										AS	GUARANTEEWAY       	 --������ʽ
								,QUALFLAGSTD       										AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB      										AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,GUARANTEETYPESTD  										AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,GUARANTORSDVSSTD  										AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,GUARANTEETYPEIRB  										AS	GUARANTEETYPEIRB     --��������֤����
								,GUARANTEEAMOUNT   										AS	GUARANTEEAMOUNT      --��֤�ܶ�
								,CURRENCY          					  				AS	CURRENCY             --����
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
								         										          AS	STARTDATE            --��ʼ����
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
								           										        AS	DUEDATE              --��������
								,ORIGINALMATURITY                     AS  ORIGINALMATURITY   	 --ԭʼ����
								,RESIDUALM                            AS	RESIDUALM            --ʣ������
								,GUARANTORIRATING  									  AS	GUARANTORIRATING     --��֤���ڲ�����
								,GUARANTORPD       									  AS	GUARANTORPD          --��֤��ΥԼ����
								,GROUPID           										AS	GROUPID              --������
    FROM 				RWA_DEV.RWA_TZ_GUARANTEE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*�����ʲ�֤ȯ�����л����ı�֤��Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_GUARANTEE(
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
         				 DATADATE          										AS  DATADATE          		 --��������
         				,DATANO            						 		    AS	DATANO               --������ˮ��
         				,GUARANTEEID       										AS	GUARANTEEID          --��֤ID
								,SSYSID            									  AS	SSYSID               --ԴϵͳID
								,GUARANTEECONID    										AS	GUARANTEECONID       --��֤��ͬID
								,GUARANTORID       										AS	GUARANTORID          --��֤��ID
								,CREDITRISKDATATYPE								    AS	CREDITRISKDATATYPE   --���÷�����������
								,GUARANTEEWAY      										AS	GUARANTEEWAY       	 --������ʽ
								,QUALFLAGSTD       										AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB      										AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,GUARANTEETYPESTD  										AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,GUARANTORSDVSSTD  										AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,GUARANTEETYPEIRB  										AS	GUARANTEETYPEIRB     --��������֤����
								,GUARANTEEAMOUNT   										AS	GUARANTEEAMOUNT      --��֤�ܶ�
								,CURRENCY          					  				AS	CURRENCY             --����
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
								         										          AS	STARTDATE            --��ʼ����
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
								           										        AS	DUEDATE              --��������
								,ORIGINALMATURITY                     AS  ORIGINALMATURITY   	 --ԭʼ����
								,RESIDUALM                            AS	RESIDUALM            --ʣ������
								,GUARANTORIRATING  									  AS	GUARANTORIRATING     --��֤���ڲ�����
								,GUARANTORPD       									  AS	GUARANTORPD          --��֤��ΥԼ����
								,GROUPID           										AS	GROUPID              --������
    FROM 				RWA_DEV.RWA_ABS_ISSURE_GUARANTEE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    
    
    
    ----------�����ҵծ �������е��� by zbwang----
    insert into RWA_EI_GUARANTEE
      (DATADATE,
       DATANO,
       GUARANTEEID,
       SSYSID,
       GUARANTEECONID,
       GUARANTORID,
       CREDITRISKDATATYPE,
       GUARANTEEWAY,
       QUALFLAGSTD,
       QUALFLAGFIRB,
       GUARANTEETYPESTD,
       GUARANTORSDVSSTD,
       GUARANTEETYPEIRB,
       GUARANTEEAMOUNT,
       CURRENCY,
       STARTDATE,
       DUEDATE,
       ORIGINALMATURITY,
       RESIDUALM,
       GUARANTORIRATING,
       GUARANTORPD,
       GROUPID)
    select 
      to_date(p_data_dt_str,'yyyymmdd'),
       p_data_dt_str,
       'B201801105279',
       'TZ',
       'B201801105279',
       'ty2017011200000051', --��֤��ID
       '01',
       '010',    ---������ʽ ��֤
       '1',      --Ȩ�ط��ϸ��ʾ
       '1',      --�����ϸ��ʾ
       '020101', --Ȩ�ط���֤����
       '06',
       '020201', --��������֤����
       a.Onsheetbalance, ---������ ��Ϊ��������Ѿ����Ϲ��ʼ�ֵ��Ӧ����Ϣ���빴��
       a.currency,
       a.startdate,
       a.duedate,
       a.originalmaturity,
       a.residualm,
       null,
       null,
       null
       from rwa_ei_exposure a where exposureid='B201801105279' 
       and datano=p_data_dt_str;
       
      
       ------�����ҵծ  �������б�֤
       insert into RWA_EI_CMRELEVENCE
       (DATADATE,
        DATANO,
        CONTRACTID,
        MITIGATIONID,
        MITIGCATEGORY,
        SGUARCONTRACTID,
        GROUPID)
        values
           (to_date(p_data_dt_str, 'yyyymmdd'),
            p_data_dt_str,
           'B201801105279',
           'B201801105279',
           '02',
           'B201801105279',
            NULL);
       commit;


       
    --�������Ϣ
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_GUARANTEE',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_GUARANTEE',partname => 'GUARANTEE'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_GUARANTEE WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_GUARANTEE��ǰ��������ݼ�¼Ϊ:' || v_count1 || '��');


		--Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

		p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '���ܱ�֤(RWA_DEV.PRO_RWA_EI_GUARANTEE)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_GUARANTEE;
/

