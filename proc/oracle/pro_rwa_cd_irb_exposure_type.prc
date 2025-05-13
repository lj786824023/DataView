CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_IRB_EXPOSURE_TYPE(
                                 p_data_dt_str  IN   VARCHAR2,    --��������
                                 p_po_rtncode   OUT  VARCHAR2,    --���ر��
                                 p_po_rtnmsg    OUT  VARCHAR2     --��������
                                 )
AS
/*
    �洢��������:PRO_RWA_CD_IRB_EXPOSURE_TYPE
    ʵ�ֹ���:���±�¶������������¶�������������¶С��
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :QHJIANG
    ��дʱ��:2017-04-05
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :���÷��ձ�¶��       RWA_EI_EXPOSURE
    Դ  ��2 :���÷��պ�ͬ��       RWA_EI_CONTRACT
    Դ  ��4 :���������           RWA_EI_CLIENT
    Ŀ���  :��������¶����(RWAϵͳ�ڲ�����)��       RWA_IRB_EXPOSURE_TYPE
    ������  :��������¶����ӳ��� RWA_CD_IRB_EXPOSURE_TYPE
    ��   ע :�ʲ�֤ȯ�����ձ�¶,�ɲ�¼����ʱ��ʼ����ɣ��ڴ˲���Ҫ������
             �������ձ�¶,�ϸ��빫˾Ӧ���˿���޴�ҵ�񣬽����еĻ������룩
    �����¼(�޸���|�޸�ʱ��|�޸�����)��
  */
    --�����쳣����
  v_raise EXCEPTION;
  --������µ�sql���
  v_update_sql VARCHAR2(4000);
  --����ƥ�������ļ�¼
  v_count number(18);
  --�������ڴ���ж�����

  --��������С��
  CLIENT_SUB_TYPE VARCHAR2(4000);
  --רҵ��������
  SL_TYPE VARCHAR2(4000);
  --ҵ��Ʒ��
  BUSINESS_TYPE VARCHAR2(4000);
  --���ϼ�ܱ�׼��С΢��ҵ
  SSMB_FLAG VARCHAR2(4000);
  --��Ŀ
  SUBJECT_NO VARCHAR2(4000);
  --���ÿ����+��ͬ���
  BALANCE VARCHAR2(4000);

  --��������¶����
  EXPO_CLASS_IRB VARCHAR2(100);
  --��������¶С��
  EXPO_SUBCLASS_IRB VARCHAR2(100);
  --�������Ʊ���
  v_pro_name VARCHAR2(200) := 'RWA_DEV.RWA_IRB_EXPOSURE_TYPE';


BEGIN

     --���������ݣ�֧������
     --DELETE FROM RWA_DEV.RWA_TEMP_CRED_LIMIT;
     --COMMIT;

     EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TEMP_CRED_LIMIT';

     BEGIN
     --��ƽ���ԱȽ������˶������ݣ�Ҫ���ɷ�����
     --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
     EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_IRB_EXPOSURE_TYPE DROP PARTITION RWA_IRB_EXPOSURE_TYPE' || p_data_dt_str;

     --COMMIT;

     EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --�״η���truncate�����2149�쳣
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '�������ԱȽ����('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --����һ����ǰ�����µķ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_IRB_EXPOSURE_TYPE ADD PARTITION RWA_IRB_EXPOSURE_TYPE' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';
    --COMMIT;

     INSERT INTO RWA_DEV.RWA_TEMP_CRED_LIMIT(CLIENTID,CRED_LIMIT)
     SELECT CLIENTID,SUM(CRED_LIMIT) AS CRED_LIMIT FROM (
            SELECT T1.CLIENTID AS CLIENTID,T1.CONTRACTAMOUNT AS CRED_LIMIT
              FROM RWA_DEV.RWA_EI_CONTRACT T1
             WHERE T1.DATADATE = TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
               AND T1.GUARANTEETYPE='005'
               AND T1.BUSINESSTYPEID in('11103018','11103015','11106010')--11103018 ������ѭ������ 11103015 ������ 11106010 ���ÿ����
            )
      GROUP BY CLIENTID;
    COMMIT;

    --δʹ�ö�ȿͻ������ǲ������浱��
    INSERT INTO RWA_DEV.RWA_TEMP_CRED_LIMIT(CLIENTID,CRED_LIMIT)
     SELECT CLIENTID,SUM(CRED_LIMIT) AS CRED_LIMIT FROM (
            SELECT T1.CLIENTID AS CLIENTID,T1.CONTRACTAMOUNT AS CRED_LIMIT
              FROM RWA_DEV.RWA_EI_CONTRACT T1
             WHERE T1.DATADATE = TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
               AND T1.GUARANTEETYPE='005'
               AND T1.BUSINESSTYPEID in('11103018','11103015','11106020')--11103018 ������ѭ������ 11103015 ������ 11106020 ���ÿ����_δʹ�ö��
            ) T
      WHERE NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_TEMP_CRED_LIMIT T2 WHERE T.CLIENTID=T2.CLIENTID)
      GROUP BY T.CLIENTID;
    COMMIT;

	dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TEMP_CRED_LIMIT',cascade => true);

  DECLARE
   --ͬ���α��ȡ��Ҫ���ж�����
  CURSOR c_cursor IS
    SELECT
         --��������С��
         CASE WHEN CLIENTSUBTYPE IS NOT NULL
              THEN ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.CLIENTID = T1.CLIENTID AND T.DATADATE = T1.DATADATE AND T1.CLIENTSUBTYPE like '''||CLIENTSUBTYPE||'%'') '
              ELSE ''
         END  AS CLIENT_SUB_TYPE
         --רҵ��������
        ,CASE WHEN SLTYPE IS NOT NULL
              THEN ' AND T.SLTYPE= '''||SLTYPE||''' '
              ELSE ''
         END  AS SL_TYPE
         --ҵ��Ʒ��
        ,CASE WHEN BUSINESSTYPE IS NOT NULL
              THEN ' AND T.BUSINESSTYPEID= '''||BUSINESSTYPE||''' '
              ELSE ''
         END  AS BUSINESS_TYPE
         --���ϼ�ܱ�׼��С΢��ҵ
        ,CASE WHEN SSMBFLAG IS NOT NULL
              THEN ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.CLIENTID = T1.CLIENTID AND T.DATADATE = T1.DATADATE AND T1.SSMBFLAG= '''||SSMBFLAG||''') '
              ELSE ''
         END  AS SSMB_FLAG
         --��Ŀ
        ,CASE WHEN SUBJECTNO IS NOT NULL
              THEN ' AND T.ACCSUBJECT1 like '''||SUBJECTNO||'%'' '
              ELSE ''
         END  AS SUBJECT_NO
         --������ʽ ���ÿ����+��ͬ���
        ,CASE WHEN BALANCE IS NOT NULL   --����:CreditAmountType 01 <=100��
               AND (   BUSINESSTYPE='11103018'--11103018 ������ѭ������
                    OR BUSINESSTYPE='11103015'--11103015 ������
                    OR BUSINESSTYPE='11106010'--11106010 ���ÿ����
                    OR BUSINESSTYPE='11106020'--11106020 ���ÿ����_δʹ�ö��
                   )
               AND BALANCE='01'
              THEN ' AND T.BUSINESSTYPEID='''||BUSINESSTYPE||'''
                     AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CONTRACT T1 WHERE T.DATADATE=T1.DATADATE AND T.CONTRACTID = T1.CONTRACTID AND ((T1.GUARANTEETYPE=''005'' AND T1.BUSINESSTYPEID in(''11103018'',''11103015'')) OR T1.BUSINESSTYPEID in(''11106010'',''11106020'') ))
                     AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_TEMP_CRED_LIMIT T1 WHERE T1.CLIENTID = T.CLIENTID AND T1.CRED_LIMIT<=1000000) '
              ELSE ''
         END  AS BALANCE
         --��������¶����
        ,EXPOCLASSIRB    AS EXPO_CLASS_IRB
         --��������¶С��
        ,EXPOSUBCLASSIRB AS EXPO_SUBCLASS_IRB
    FROM RWA_CD_IRB_EXPOSURE_TYPE
    ORDER BY SORTNO ASC
    ;

  BEGIN
    --�����α�
    OPEN c_cursor;
    --ͨ��ѭ�������������α�
    LOOP
      --���α��ȡ��ֵ���趨���ƥ������
      FETCH c_cursor INTO
        CLIENT_SUB_TYPE    --��������С��
       ,SL_TYPE            --רҵ��������
       ,BUSINESS_TYPE      --ҵ��Ʒ��
       ,SSMB_FLAG          --���ϼ�ܱ�׼��С΢��ҵ
       ,SUBJECT_NO         --��Ŀ
       ,BALANCE            --���ÿ����+��ͬ���
       ,EXPO_CLASS_IRB     --��������¶����
       ,EXPO_SUBCLASS_IRB  --��������¶С��
       ;
      --���α������ɺ��˳��α�
      EXIT WHEN c_cursor%NOTFOUND;
      v_update_sql:='INSERT INTO RWA_DEV.RWA_IRB_EXPOSURE_TYPE(DATADATE,DATANO,EXPOSUREID,EXPOCLASSIRB,EXPOSUBCLASSIRB,RWAEXPOCLASSIRB,RWAEXPOSUBCLASSIRB)
                     SELECT  T.DATADATE                     AS DATADATE
                            ,T.DATANO                       AS DATANO
                            ,T.EXPOSUREID                   AS EXPOSUREID
                            ,T.EXPOCLASSIRB                 AS EXPOCLASSIRB
                            ,T.EXPOSUBCLASSIRB              AS EXPOSUBCLASSIRB
                            ,'''||EXPO_CLASS_IRB||'''       AS RWAEXPOCLASSIRB
                            ,'''||EXPO_SUBCLASS_IRB||'''    AS RWAEXPOSUBCLASSIRB
                       FROM RWA_DEV.RWA_EI_EXPOSURE T
                      WHERE T.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'')
                        AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_IRB_EXPOSURE_TYPE RIET WHERE RIET.DATANO=T.DATANO AND RIET.EXPOSUREID=T.EXPOSUREID) ';

      --�ϲ�sql��ƴ��where����
      v_update_sql := v_update_sql
      ||CLIENT_SUB_TYPE    --��������С��
      ||SL_TYPE            --רҵ��������
      ||BUSINESS_TYPE      --ҵ��Ʒ��
      ||SSMB_FLAG          --���ϼ�ܱ�׼��С΢��ҵ
      ||SUBJECT_NO         --��Ŀ
      ||BALANCE            --���ÿ����+��ͬ���
      ;

      EXECUTE IMMEDIATE v_update_sql;
      COMMIT;
      --����ѭ��
    END LOOP;
    --�ر��α�
    CLOSE c_cursor;
  END;
    ----�������Ϣ
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_IRB_EXPOSURE_TYPE',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_IRB_EXPOSURE_TYPE',partname => 'RWA_IRB_EXPOSURE_TYPE'||p_data_dt_str,granularity => 'PARTITION',cascade => true);

    --ִ�и��±�¶��С��֮ǰ�������б�¶��С���ÿգ�ʹ�����ǵĹ��������������¶���ࣩ
    UPDATE RWA_DEV.RWA_EI_EXPOSURE SET EXPOCLASSIRB=NULL,EXPOSUBCLASSIRB=NULL
    WHERE DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND SSYSID <> 'LC'
    AND EXPOSUREID <> 'B200801010095';

    COMMIT;

    --������������¶��С��(��������¶��С��Ϊ�յ����)
    MERGE INTO (SELECT DATADATE,DATANO,EXPOSUREID,EXPOCLASSIRB,EXPOSUBCLASSIRB
                  FROM RWA_DEV.RWA_EI_EXPOSURE
                 WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                   AND EXPOCLASSIRB IS NULL) T
    USING (SELECT T1.EXPOSUREID,T1.DATADATE,T1.DATANO,T1.RWAEXPOCLASSIRB,T1.RWAEXPOSUBCLASSIRB
             FROM RWA_DEV.RWA_IRB_EXPOSURE_TYPE T1
            WHERE T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')) T2
    ON(T2.EXPOSUREID = T.EXPOSUREID AND T2.DATADATE = T.DATADATE AND T2.DATANO=T.DATANO )
    WHEN MATCHED THEN
      UPDATE SET T.EXPOCLASSIRB = T2.RWAEXPOCLASSIRB,T.EXPOSUBCLASSIRB = T2.RWAEXPOSUBCLASSIRB;

		COMMIT;

    --����������¶�����ȫ���ŵ�һ�㹫˾
    UPDATE RWA_DEV.RWA_EI_EXPOSURE T SET T.EXPOCLASSIRB = '0203',T.EXPOSUBCLASSIRB = '020301'
    WHERE T.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND T.EXPOSUBCLASSIRB IS NULL;

    COMMIT;

    --������������׼С΢��ҵ��ʶΪ1�Ŀͻ�����������¶���Ϊ�������۷��ձ�¶
		UPDATE RWA_DEV.RWA_EI_CLIENT SET EXPOCATEGORYIRB = '020403'
		WHERE  DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')
		AND		 CLIENTTYPE = '03'
		AND		 SSMBFLAG = '1'
		;

		COMMIT;

		--���²���������������¶��С��
    MERGE INTO (SELECT CLIENTID
    									,EXPOCATEGORYIRB
    							FROM RWA_DEV.RWA_EI_CLIENT
    						 WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    						 	 AND EXPOCATEGORYIRB IS NULL) T
    	USING (SELECT T1.CLIENTID,
              MAX(T1.EXPOSUBCLASSIRB) AS EXPOSUBCLASSIRB
         FROM RWA_DEV.RWA_EI_EXPOSURE T1
        WHERE T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
        GROUP BY T1.CLIENTID) T2
     	ON (T.CLIENTID = T2.CLIENTID)
		WHEN MATCHED THEN
  	UPDATE SET T.EXPOCATEGORYIRB = T2.EXPOSUBCLASSIRB;

		COMMIT;


		--����û�б�¶�Ĳ���������������¶���
	  MERGE INTO (SELECT CLIENTSUBTYPE,EXPOCATEGORYIRB FROM RWA_DEV.RWA_EI_CLIENT WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') AND EXPOCATEGORYIRB IS NULL) T
	  	USING (SELECT T1.CLIENTSUBTYPE,MIN(T1.EXPOSUBCLASSIRB) AS EXPOSUBCLASSIRB
	         FROM RWA_DEV.RWA_CD_IRB_EXPOSURE_TYPE T1
	        GROUP BY T1.CLIENTSUBTYPE) T2
	  	ON (T.CLIENTSUBTYPE = T2.CLIENTSUBTYPE)
	  WHEN MATCHED THEN
	  UPDATE SET T.EXPOCATEGORYIRB = T2.EXPOSUBCLASSIRB;

		COMMIT;
    
    
   

    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_IRB_EXPOSURE_TYPE WHERE DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD');
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '������������¶��С��(PRO_RWA_CD_IRB_EXPOSURE_TYPE)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_IRB_EXPOSURE_TYPE;
/

