CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_UPDATE_SSMBFLAG(
                                                      p_data_dt_str  IN   VARCHAR2,   --��������
                                                      p_po_rtncode   OUT  VARCHAR2,   --���ر��
                                                      p_po_rtnmsg    OUT  VARCHAR2    --��������
                                                      )
  /*
    �洢��������:PRO_RWA_CD_UPDATE_SSMBFLAG
    ʵ�ֹ���:���±�׼΢С��ҵ��ʶ
    ���ݿھ�:ȫ��
    ����Ƶ��:��ĩ����
    ��  ��  :V1.0.0
    ��д��  :YUSHUANGJIANG
    ��дʱ��:2017-08-11
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Ŀ���  :RWA_EI_CLIENT
    ������1 :
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_CD_UPDATE_SSMBFLAG';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN

    --��ʱ��ֻ����һ�����ݣ�����֮ǰɾ��������,��������֮ǰɾ������
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TEMP_CLIENT_SSMBFLAG';

    --��������,ȫ����¶���տͻ�����
    INSERT INTO RWA_DEV.RWA_TEMP_CLIENT_SSMBFLAG(
                CLIENTID
                ,BALANCE
    )
    SELECT T1.CLIENTID
          ,SUM(CASE WHEN T1.EXPOBELONG='02' THEN NVL(T1.NORMALPRINCIPAL,0) * NVL(T2.CCF,1)
                    ELSE NVL(T1.NORMALPRINCIPAL,0)
               END) AS BALANCE
    FROM RWA_DEV.RWA_EI_EXPOSURE T1
    LEFT JOIN RWA_DEV.RWA_CD_OFF_EXPOSURE_TYPE T2
    ON T1.OFFBUSINESSSDVSSTD=T2.OFFBUSINESSSDVSSTD
    WHERE T1.DATADATE=TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
    GROUP BY T1.CLIENTID
    ;
    COMMIT;

    --���¼�����ؿͻ��ı�¶���
    MERGE INTO RWA_DEV.RWA_TEMP_CLIENT_SSMBFLAG T1
		USING (SELECT TGM.MEMBERID 			AS MEMBERID
								 ,MAX(TG.BALANCE)	 	AS BALANCE
		         FROM RWA_DEV.TMP_GROUP_MEMBERS TGM
		        INNER JOIN (SELECT T2.GROUPID
		        									,SUM(T1.BALANCE) AS BALANCE
		                     FROM RWA_DEV.RWA_TEMP_CLIENT_SSMBFLAG T1
		                    INNER JOIN RWA_DEV.TMP_GROUP_MEMBERS T2
		                       ON T1.CLIENTID = T2.MEMBERID
		                    GROUP BY T2.GROUPID) TG
		           ON TGM.GROUPID = TG.GROUPID
		           GROUP BY TGM.MEMBERID) T2
		ON (T1.CLIENTID = T2.MEMBERID)
		WHEN MATCHED THEN
		  UPDATE
		     SET T1.BALANCE = T2.BALANCE
		;

		COMMIT;

		dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TEMP_CLIENT_SSMBFLAG',cascade => true);

    --���²���������ܱ��Ȩ�ط���׼΢С��ҵ�ֶ�
    --���²���������ܱ��Ȩ�ط���׼΢С��ҵ�ֶ�
    MERGE INTO RWA_DEV.RWA_EI_CLIENT T1
    USING (SELECT A.CLIENTID,A.BALANCE,B.ORGNATURE,C.COMPANYSIZE
           FROM RWA_DEV.RWA_TEMP_CLIENT_SSMBFLAG A
           INNER JOIN RWA_DEV.NCM_CUSTOMER_INFO B   --�������߼����������ͱ������� ������ҵ �Ƿ�����ҵ ���幤�̻� --modify by yushuangjiang
           ON A.CLIENTID=B.CUSTOMERID
           AND B.DATANO=P_DATA_DT_STR
           AND B.ORGNATURE IN('0101','0102') --������ҵ �Ƿ�����ҵ --���幤�̻� ȥ��
           INNER JOIN RWA_DEV.RWA_EI_CLIENT C
           ON A.CLIENTID=C.CLIENTID
           AND C.DATANO=P_DATA_DT_STR
           AND C.COMPANYSIZE IN ('02','03')
    			 AND C.CLIENTTYPE = '03'
           WHERE A.BALANCE <= 10000000) T2
    ON (T1.CLIENTID = T2.CLIENTID)
    WHEN MATCHED THEN
    UPDATE SET T1.SSMBFLAGSTD = '1'
    ;

    COMMIT;

		--������С��ҵĬ�ϵ�����ƽ�������۶�
		UPDATE RWA_DEV.RWA_EI_CLIENT SET ANNUALSALE = 300000000
		WHERE  DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')
		AND		 CLIENTSUBTYPE = '0302'
		AND		 (ANNUALSALE IS NULL OR ANNUALSALE = 0)
		;

		COMMIT;

		--������������׼С΢��ҵ�±�¶����������¶���ࡢ���۱�¶��������÷�����������
		MERGE INTO (SELECT CLIENTID,EXPOCLASSIRB,EXPOSUBCLASSIRB,RETAILEXPOFLAG,RETAILCLAIMTYPE,CREDITRISKDATATYPE FROM RWA_DEV.RWA_EI_EXPOSURE WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) T
		USING (SELECT CLIENTID FROM RWA_DEV.RWA_EI_CLIENT WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD') AND SSMBFLAG = '1' AND CLIENTTYPE = '03') T1
		ON (T.CLIENTID = T1.CLIENTID)
		WHEN MATCHED THEN
			UPDATE SET T.EXPOCLASSIRB = '0204', T.EXPOSUBCLASSIRB = '020403',T.RETAILEXPOFLAG = '1',T.RETAILCLAIMTYPE = '020403',T.CREDITRISKDATATYPE = '02'
		;

		COMMIT;

		--������������׼С΢��ҵ�º�ͬ�����÷�����������
		MERGE INTO (SELECT CLIENTID,CREDITRISKDATATYPE FROM RWA_DEV.RWA_EI_CONTRACT WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) T
		USING (SELECT CLIENTID FROM RWA_DEV.RWA_EI_CLIENT WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD') AND SSMBFLAG = '1' AND CLIENTTYPE = '03') T1
		ON (T.CLIENTID = T1.CLIENTID)
		WHEN MATCHED THEN
			UPDATE SET T.CREDITRISKDATATYPE = '02'
		;

		COMMIT;

		--���Թ������۹���ѺƷʱ��ֻ���Թ��ã�ɾ�����۲��ֶ�Ӧ��ϵ����ɾ����Ӧ��ѺƷ��Ϣ
    DELETE FROM RWA_DEV.RWA_EI_CMRELEVENCE REC
    WHERE  REC.MITIGATIONID IN
           (SELECT MITIGATIONID
            FROM (SELECT     DISTINCT T1.CREDITRISKDATATYPE, T.MITIGATIONID
                  FROM       RWA_DEV.RWA_EI_CMRELEVENCE T
                  INNER JOIN RWA_DEV.RWA_EI_CONTRACT T1
                  ON         T1.CONTRACTID = T.CONTRACTID
                  AND        T1.DATADATE = T.DATADATE
                  AND        T1.DATANO = T.DATANO
                  WHERE      T.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD'))
            GROUP BY MITIGATIONID
            HAVING count(1) > 1)
    AND EXISTS (SELECT 1
                FROM  RWA_DEV.RWA_EI_CONTRACT CON
                WHERE CON.DATADATE = REC.DATADATE
                AND   CON.DATANO = REC.DATANO
                AND   CON.CONTRACTID = REC.CONTRACTID
                AND   CON.CREDITRISKDATATYPE = '02')
    AND REC.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD');

    COMMIT;

    --ɾ������ĵ���ѺƷ
    DELETE FROM RWA_DEV.RWA_EI_COLLATERAL T
    WHERE	 T.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')
    AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CMRELEVENCE T1 WHERE T.COLLATERALID = T1.MITIGATIONID AND T.DATADATE = T1.DATADATE)
    ;

    COMMIT;

    --ɾ������ı�֤
    DELETE FROM RWA_DEV.RWA_EI_GUARANTEE T
    WHERE	 T.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')
    AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CMRELEVENCE T1 WHERE T.GUARANTEEID = T1.MITIGATIONID AND T.DATADATE = T1.DATADATE)
    ;

    COMMIT;

    --��������ѺƷ��ϵ�����÷�����������--����ѺƷ
    MERGE INTO (SELECT COLLATERALID, CREDITRISKDATATYPE
		              FROM RWA_DEV.RWA_EI_COLLATERAL
		             WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) T
		USING (SELECT DISTINCT REC.MITIGATIONID AS MITIGATIONID
		         FROM RWA_DEV.RWA_EI_CMRELEVENCE REC
		        INNER JOIN RWA_DEV.RWA_EI_CONTRACT REN
		           ON REC.CONTRACTID = REN.CONTRACTID
		          AND REC.DATADATE = REN.DATADATE
		          AND REN.CREDITRISKDATATYPE = '02'
		        WHERE REC.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')
		          AND REC.MITIGCATEGORY = '03') T1
		ON (T.COLLATERALID = T1.MITIGATIONID)
		WHEN MATCHED THEN
			UPDATE SET T.CREDITRISKDATATYPE = '02'
		;

    COMMIT;

    --��������ѺƷ��ϵ�����÷�����������--��֤
    MERGE INTO (SELECT GUARANTEEID, CREDITRISKDATATYPE
		              FROM RWA_DEV.RWA_EI_GUARANTEE
		             WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) T
		USING (SELECT DISTINCT REC.MITIGATIONID AS MITIGATIONID
		         FROM RWA_DEV.RWA_EI_CMRELEVENCE REC
		        INNER JOIN RWA_DEV.RWA_EI_CONTRACT REN
		           ON REC.CONTRACTID = REN.CONTRACTID
		          AND REC.DATADATE = REN.DATADATE
		          AND REN.CREDITRISKDATATYPE = '02'
		        WHERE REC.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')
		          AND REC.MITIGCATEGORY = '02') T1
		ON (T.GUARANTEEID = T1.MITIGATIONID)
		WHEN MATCHED THEN
			UPDATE SET T.CREDITRISKDATATYPE = '02'
		;

    COMMIT;

    --
    MERGE INTO (SELECT REE.CONTRACTID,
		                   REE.CCFAIRB,
		                   REE.PD,
		                   REE.LGDAIRB,
		                   REE.DEFAULTFLAG,
		                   REE.BEEL,
		                   REE.DEFAULTLGD,
		                   REE.AGING,
		                   REE.PDPOOLMODELID,
		                   REE.LGDPOOLMODELID,
		                   REE.CCFPOOLMODELID,
		                   REE.PDPOOLID,
		                   REE.LGDPOOLID,
		                   REE.CCFPOOLID,
		                   REE.DefaultDate,
		                   REE.EADAIRB,
		                   REE.EXPOBELONG,
		                   REE.ASSETBALANCE
		              FROM RWA_DEV.RWA_EI_EXPOSURE REE
		             WHERE REE.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')
		               AND EXISTS (SELECT 1
		                      FROM RWA_DEV.RWA_EI_CLIENT REC
		                     WHERE REE.CLIENTID = REC.CLIENTID
		                       AND REE.DATADATE = REC.DATADATE
		                       AND REC.SSMBFLAG = '1'
		                       AND REC.CLIENTTYPE = '03')) T
		USING (SELECT BUSINESSID,
		              CCFVALUE,
		              PDVALUE,
		              LGDVALUE,
		              DEFAULTFLAG,
		              BEELVALUE,
		              MOB,
		              PDMODELCODE,
		              LGDMODELCODE,
		              CCFMODELCODE,
		              PDCODE,
		              LGDCODE,
		              CCFCODE,
		              UPDATETIME
		         FROM RWA_DEV.RWA_TEMP_LGDLEVEL
		        WHERE BUSINESSTYPE <> 'CREDITCARD') T1
		ON (T.CONTRACTID = T1.BUSINESSID)
		WHEN MATCHED THEN
		  UPDATE
		     SET T.CCFAIRB        = T1.CCFVALUE,
		         T.PD             = T1.PDVALUE,
		         T.LGDAIRB        = T1.LGDVALUE,
		         T.DEFAULTFLAG    = NVL(T1.DEFAULTFLAG, '0'),
		         T.BEEL           = T1.BEELVALUE,
		         T.DEFAULTLGD     = T1.LGDVALUE,
		         T.AGING          = T1.MOB,
		         T.PDPOOLMODELID  = T1.PDMODELCODE,
		         T.LGDPOOLMODELID = T1.LGDMODELCODE,
		         T.CCFPOOLMODELID = T1.CCFMODELCODE,
		         T.PDPOOLID       = T1.PDCODE,
		         T.LGDPOOLID      = T1.LGDCODE,
		         T.CCFPOOLID      = T1.CCFCODE,
		         T.DEFAULTDATE    = CASE WHEN NVL(T1.DEFAULTFLAG, '0') = '1' THEN TO_DATE(T1.UPDATETIME,'YYYYMMDD') ELSE NULL END,
		         T.EADAIRB        = T.ASSETBALANCE * CASE WHEN T.EXPOBELONG = '02' THEN NVL(T1.CCFVALUE, 1) ELSE 1 END
		;

    COMMIT;

    --
    UPDATE RWA_DEV.RWA_EI_EXPOSURE REE
		   SET REE.CCFAIRB        = NULL,
		       REE.PD             = NULL,
		       REE.LGDAIRB        = NULL,
		       REE.DEFAULTFLAG    = '0',
		       REE.BEEL           = NULL,
		       REE.DEFAULTLGD     = NULL,
		       REE.AGING          = NULL,
		       REE.PDPOOLMODELID  = NULL,
		       REE.LGDPOOLMODELID = NULL,
		       REE.CCFPOOLMODELID = NULL,
		       REE.PDPOOLID       = NULL,
		       REE.LGDPOOLID      = NULL,
		       REE.CCFPOOLID      = NULL,
		       REE.DEFAULTDATE    = NULL,
		       REE.EADAIRB        = REE.ASSETBALANCE
		 WHERE REE.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')
		   AND EXISTS (SELECT 1
		          FROM RWA_DEV.RWA_EI_CLIENT REC
		         WHERE REE.CLIENTID = REC.CLIENTID
		           AND REE.DATADATE = REC.DATADATE
		           AND REC.SSMBFLAG = '1'
		           AND REC.CLIENTTYPE = '03')
		   AND NOT EXISTS (SELECT 1
		          FROM RWA_DEV.RWA_TEMP_LGDLEVEL T1
		         WHERE T1.BUSINESSTYPE <> 'CREDITCARD'
		           AND REE.CONTRACTID = T1.BUSINESSID)
		;

    COMMIT;


    --����Ȩ�ط���׼΢С��ҵ�� ���ǡ�������
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_CLIENT WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD') AND SSMBFLAGSTD = '1';
    --�����쳣
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�-' || v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
         ROLLBACK;
         p_po_rtncode := sqlcode;
         p_po_rtnmsg  := '���������׼С΢��ҵ��ʶ����('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_UPDATE_SSMBFLAG;
/

