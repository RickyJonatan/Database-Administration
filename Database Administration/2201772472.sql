--1
USE [msdb]
/****** Object:  Job [Quarterly Report]    Script Date: 12/06/2020 02:25:32 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Data Collector]    Script Date: 12/06/2020 02:25:32 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Data Collector' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Quarterly Report', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Generate quarterly report of all transactions that occur', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Quarterly Report]    Script Date: 12/06/2020 02:25:33 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Quarterly Report', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SELECT DISTINCT ht.TransactionID, mc.CustomerName, ms.StaffName, ht.TransactionDate, 
[TotalItem] = COUNT(mp.PaymentTypeID), [TotalQuantity] = SUM(dt.Quantity), [TotalPurchase] = SUM(mi.ItemPrice*dt.Quantity)
FROM HeaderTransaction ht JOIN MsCustomer mc
ON mc.CustomerID = ht.CustomerID JOIN MsStaff ms
ON ms.StaffID = ht.StaffID JOIN MsPaymentType mp
ON mp.PaymentTypeID = ht.PaymentTypeID JOIN DetailTransaction dt
ON ht.TransactionID = dt.TransactionID JOIN MsItem mi
ON dt.ItemID = mi.ItemID WHERE MONTH(ht.TransactionDate) = 4 AND YEAR(ht.TransactionDate)= 2020
GROUP BY ht.TransactionID, mc.CustomerName, ms.StaffName, ht.TransactionDate
', 
		@database_name=N'Sociolla', 
		@output_file_name=N'D:\ReportDetails.txt', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Schedule', 
		@enabled=1, 
		@freq_type=32, 
		@freq_interval=8, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=16, 
		@freq_recurrence_factor=3, 
		@active_start_date=20200611, 
		@active_end_date=99991231, 
		@active_start_time=230000, 
		@active_end_time=235959, 
		@schedule_uid=N'c20664ee-6842-4268-a917-11dfd153fbca'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO




---2
USE [msdb]
GO

/****** Object:  Job [Remaining Stock Report]    Script Date: 12/06/2020 02:46:20 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Data Collector]    Script Date: 12/06/2020 02:46:20 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Data Collector' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Remaining Stock Report', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Generate a report to print out the remaining stock of every item', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Remaining Stock Report]    Script Date: 12/06/2020 02:46:20 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Remaining Stock Report', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SELECT mi.ItemID ,mi.Itemname,[Remaining]=ItemStock- Sum(dt.Quantity) FROM DetailTransaction dt JOIN MsItem mi on dt.ItemID = mi.ItemID
Group BY mi.ItemID,mi.ItemName,mi.ItemStock
', 
		@database_name=N'Sociolla', 
		@output_file_name=N'D:\RemainingStock.txt', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Remaining Stock Report', 
		@enabled=1, 
		@freq_type=32, 
		@freq_interval=10, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=16, 
		@freq_recurrence_factor=2, 
		@active_start_date=20200611, 
		@active_end_date=99991231, 
		@active_start_time=223000, 
		@active_end_time=235959, 
		@schedule_uid=N'4f3c5d57-4201-4dab-b7b2-a88d31c289c3'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO







--3
CREATE PROC ValidateItem @ID CHAR(5), @NAME VARCHAR(50), @STOCK INT, @PRICE BIGINT, @BRAND CHAR(5), @CATEGORY CHAR(5)
AS 
 IF EXISTS(SELECT * FROM MsItem WHERE ItemID = @ID AND ItemName = @NAME)
 BEGIN
  PRINT 'Item already exists'
 END
 ELSE IF EXISTS(SELECT * FROM MsItem WHERE ItemName = @NAME)
 BEGIN
  PRINT 'This item already exists with different ID'
 END
 ELSE IF EXISTS(SELECT * FROM MsItem WHERE ItemID = @ID)
 BEGIN
  PRINT 'ID must be unique!'
 END
 ELSE
 BEGIN
  INSERT INTO MsItem VALUES(@ID, @NAME, @STOCK, @PRICE, @BRAND, @CATEGORY)
 END

BEGIN TRAN
 EXEC ValidateItem 'IT035', 'Mediheal Set', 23, 100000, 'IB001', 'IC001'
ROLLBACK

DROP PROC ValidateItem

--4
CREATE PROC RemoveItem @ID CHAR(5)
AS
IF NOT EXISTS(SELECT * FROM	MsItem WHERE ItemID=@ID)
BEGIN
 PRINT 'Item Doesnt exists'

END
ELSE IF NOT EXISTS(SELECT TOP 5 ItemName ,[TOTALQUANTITY]= SUM(QUANTITY) FROM DetailTransaction dt 
JOIN MsItem mi on dt.ItemID = mi.ItemID 
WHERE mi.ItemID=@ID
GROUP BY ItemName
ORDER BY SUM(QUANTITY) DESC 
)
BEGIN
 PRINT 'Item cannot deleted because it is in the Top 5'
END

ELSE
UPDATE MsItem SET ItemStock =0
WHERE ItemID=@ID
BEGIN TRAN
 EXEC RemoveItem 'IT023'
ROLLBACK

DROP PROC RemoveItem
SELECT * FROM DetailTransaction

select *from MsItem

--5
CREATE PROC DeleteItem @ID CHAR(5)
AS
IF NOT EXISTS(SELECT * FROM	MsItem WHERE ItemID=@ID)
BEGIN
 PRINT 'Item Doesnt exists'
END

ELSE IF EXISTS(SELECT * FROM DetailTransaction WHERE ItemID=@ID)
BEGIN

PRINT 'Item cannot be removed'

END

ELSE
DELETE  FROM MsItem WHERE ItemID=@ID

BEGIN TRAN
EXEC DeleteItem'IT034'
ROLLBACK




--6
Go
CREATE TRIGGER InsertItemTrigger On Msitem Instead OF INSERT 
AS 
DECLARE
@ITEMID CHAR(5),
@ITEMNAME VARCHAR(50),
@ITEMSTOCK INT,
@ITEMPRICE INT,
@ITEMBRAND CHAR(5),
@ITEMCATEGORY CHAR(5)
SELECT ItemID = @ITEMID, ItemName = @ITEMNAME, ItemStock = @ItemStock, ItemPrice = @ITEMPRICE, ItemBrandID = @ITEMBRAND, ItemCategoryID = @ITEMCATEGORY
   FROM inserted

   IF EXISTS(SELECT * FROM MsItem WHERE ItemID=@ITEMID)
   BEGIN
   PRINT 'Item ID already exists'
   END

   ELSE IF @ITEMID NOT LIKE 'IT[0-9][0-9][0-9]'
   BEGIN
   PRINT 'Item ID must be in the right format'
   END

   
   ELSE IF @ITEMSTOCK<10
   BEGIN
   PRINT 'Item Stock must be greater than 10'
   END

   ELSE IF @ITEMBRAND NOT LIKE 'IB[0-9][0-9][0-9]'
   BEGIN
   PRINT 'Item Brand must be in the right format'
   END
   
   ELSE IF @ITEMBRAND NOT IN(SELECT ItemBrandID  FROM MsItemBrand)
   BEGIN
   PRINT @ITEMBRAND + 'doesnt exist'
   END

   ELSE IF @ITEMCATEGORY NOT LIKE 'IC[0-9][0-9][0-9]'
   BEGIN
   PRINT 'Item Category must be in the right format'
   END

   ELSE IF @ITEMCATEGORY NOT IN(SELECT ItemCategoryID  FROM MsItemCategory)
   BEGIN
   PRINT @ITEMCATEGORY+ 'doesnt exist'
   END

   ELSE INSERT INTO MsItem VALUES (@ITEMID, @ITEMNAME, @ITEMSTOCK, @ITEMPRICE, @ITEMBRAND, @ITEMCATEGORY)

   DROP TRIGGER InsertItemTrigger

   BEGIN TRAN

   INSERT INTO MsItem VALUES('IT001', 'Two Tone Tint Lip Bar', 35, 368500, 'IB008', 'IC001')

   ROLLBACK

--7
CREATE TRIGGER RefundTransaction
ON HeaderTransaction
FOR UPDATE
AS
BEGIN
DECLARE @trans char(5),@bayar char(5), @tanggal date, @stok INT
SET @trans=(SELECT i.CustomerID FROM inserted i JOIN deleted d ON i.CustomerID=d.CustomerID)
SET @bayar=(SELECT d.PaymentTypeID FROM inserted i JOIN deleted d ON i.CustomerID=d.CustomerID)
SET @tanggal=(SELECT TransactionDate From HeaderTransaction ht WHERE ht.CustomerID = @trans)
SET @stok=(SELECT ItemStock FROM MsItem mi,DetailTransaction dt,HeaderTransaction ht WHERE dt.TransactionID= ht.TransactionID  AND mi.ItemID=dt.ItemID AND ht.CustomerID = @trans)
PRINT 'Old Transaction'
PRINT '==============='
PRINT 'Payment Type: '+@bayar
PRINT 'Transaction Date: '+CAST(@tanggal AS VARCHAR(30))
PRINT 'Previous Stock: '+CAST(@stok AS VARCHAR(30))
PRINT''
PRINT''	

SET @bayar=(SELECT i.PaymentTypeID FROM inserted i JOIN deleted d ON i.CustomerID=d.CustomerID)
SET @tanggal=(SELECT TransactionDate=GETDATE() FROM HeaderTransaction ht WHERE ht.CustomerID = @trans)
SET @stok=(SELECT ItemStock=ItemStock+Quantity FROM MsItem mi,DetailTransaction dt,HeaderTransaction ht WHERE dt.TransactionID= ht.TransactionID  AND mi.ItemID=dt.ItemID AND ht.CustomerID = @trans)

PRINT 'New Transaction'
PRINT '==============='
PRINT 'Payment Type: '+@bayar
PRINT 'Transaction Date: '+CAST(@tanggal AS VARCHAR(30))
PRINT 'Previous Stock: '+CAST(@stok AS VARCHAR(30))
END




--8 
GO
CREATE PROC PrintReceipt (@TransactionID CHAR(5))
AS
BEGIN
	DECLARE 
		@CustomerName VARCHAR (30),
		@TransactionDate DATE,
		@StaffName VARCHAR (30),
		@PaymentType VARCHAR(30),
		@ItemName VARCHAR (30),
		@Quantity INT,
		@ItemBrand VARCHAR (30),
		@ItemCategory VARCHAR (30),
		@ItemPrice INT,
		@Total1 INT,
		@TotalItem1 INT,
		@TotalItem INT = 0,
		@Total INT=0

	SELECT @CustomerName = CustomerName, @TransactionDate=TransactionDate, @StaffName = StaffName, @PaymentType = PaymentTypeName

	FROM HeaderTransaction ht JOIN MsCustomer mc ON mc.CustomerID = ht.CustomerID JOIN MsStaff ms ON ms.StaffID = ht.StaffID JOIN MsPaymentType mt ON mt.PaymentTypeID = ht.PaymentTypeID
	WHERE TransactionID = @TransactionID

	PRINT 'Hi, ' + @CustomerName
	PRINT 'Here are your shopping details'
	PRINT 'Transaction Date :' + CAST(CONVERT(VARCHAR,@TransactionDate, 107) AS VARCHAR)
	PRINT 'Cashier: ' + @StaffName
	PRINT 'Payment: ' + @PaymentType
	PRINT '-------------------------------------------'

	DECLARE CursorItem  CURSOR

	FOR
		SELECT ItemName, Quantity, ItemBrandName, ItemCategoryName, ItemPrice
		FROM MsItem mi JOIN DetailTransaction dt ON dt.ItemID = mi.ItemID JOIN MsItemBrand mb ON mb.ItemBrandID = mi.ItemBrandID JOIN MsItemCategory mc ON mi.ItemCategoryID  = mc.ItemCategoryID
		WHERE @TransactionID = TransactionID

	OPEN CursorItem 
	FETCH NEXT FROM CursorItem INTO @ItemName, @Quantity, @ItemBrand, @ItemCategory, @ItemPrice

		WHILE @@FETCH_STATUS=0
			BEGIN
				SET @Total1=@ItemPrice*@Quantity
				SET @TotalItem1+=@Quantity
				PRINT 'Item : ' + @ItemName
				PRINT 'Quantity : ' + CAST (@Quantity AS VARCHAR)
				PRINT 'Brand : ' + @ItemBrand
				PRINT 'Category : ' + @ItemCategory
				PRINT 'Price per item : ' + CAST(@ItemPrice AS VARCHAR)
				PRINT 'Total :' + CAST(@Total1 AS VARCHAR)
				PRINT '-------------------------------------------'

				SET @Total += @Total1
				SET @TotalItem += @TotalItem1

		FETCH NEXT FROM CursorItem INTO @ItemName, @Quantity, @ItemBrand, @ItemCategory, @ItemPrice
		END

	
		SELECT @TotalItem=COUNT(*)
		FROM MsItem 
		JOIN DetailTransaction ON DetailTransaction.ItemID = MsItem.ItemID
		WHERE TransactionID = @TransactionID
		GROUP BY TransactionID

		
		PRINT 'Total Item : ' + CAST(@TotalItem AS VARCHAR)
		PRINT 'Total Price : Rp.' + CAST(@Total AS VARCHAR)
		
		CLOSE CursorItem
		DEALLOCATE CursorItem

END

BEGIN TRAN
EXEC PrintReceipt 'TR022'

ROLLBACK


DROP proc PrintReceipt
--9
GO
CREATE PROCEDURE SearchItem (@brand VARCHAR(50))
AS
    DECLARE @ID CHAR(5)
    DECLARE @Name VARCHAR(50)
    DECLARE @Stock INT
    DECLARE @Price INT

SELECT @brand = mb.ItemBrandName
FROM MsItemBrand mb
WHERE @brand = mb.ItemBrandName

IF LEN(@brand) < 3
    PRINT 'Keyword must be longer than 3 characters'
ELSE IF @brand NOT IN (SELECT ItemBrandName FROM MsItemBrand)
    PRINT 'Brand doesn’t exist'
ELSE 
    PRINT 'Brand: ' + @brand
    PRINT '-------------------------------------------'
    PRINT '-------------------------------------------'
    DECLARE CursorBrand CURSOR 
    FOR
    SELECT mb.ItemBrandID, mi.ItemName, mi.ItemStock, mi.ItemPrice
    FROM MsItemBrand mb JOIN MsItem mi ON mb.ItemBrandID = mi.ItemBrandID
    WHERE @brand = mb.ItemBrandName

    OPEN CursorBrand
            FETCH NEXT FROM CursorBrand
            INTO @ID, @Name, @Stock, @Price
            WHILE @@FETCH_STATUS=0
            BEGIN
            PRINT 'Item ID: ' + @ID
            PRINT 'Item Name: ' + @Name
            PRINT 'Item Stock: ' + CAST(@Stock as VARCHAR) 
            PRINT 'Item Price: ' + CAST(@Price as VARCHAR) 
            PRINT '-------------------------------------------'
            PRINT '-------------------------------------------'
			FETCH NEXT FROM CursorBrand
            INTO @ID, @Name, @Stock, @Price
            END
    CLOSE CursorBrand
    DEALLOCATE CursorBrand

	begin tran
	EXEC SearchItem 'Cetaphil'
	rollback

	drop proc SearchItem


--10
GO
CREATE PROC DisplayTransaction (@Start INT, @End INT, @Year INT)
AS
	DECLARE
		@TransactionID CHAR(5),
		@TransactionDate DATE,
		@ItemName CHAR(20),
		@ItemPrice INT,
		@Quantity INT,
		@Total INT = 0,
		@Total1 INT,
		@Count INT =1,
		@Counter INT=1
	

		IF((@End - @Start) > 10)
			PRINT 'The maximum range is 10 months'
		ELSE
		BEGIN
			PRINT 'Showing results from'
			DECLARE transcursor CURSOR
			FOR
			
			SELECT TransactionID, TransactionDate
			FROM HeaderTransaction
			WHERE CAST(MONTH(TransactionDate) AS INT)>=@Start AND CAST(MONTH(TransactionDate) AS INT)<=@End AND YEAR(TransactionDate) = @Year

			OPEN transcursor
				FETCH NEXT FROM transcursor
				INTO @TransactionID, @TransactionDate

				WHILE @@FETCH_STATUS=0
					BEGIN
					PRINT '================================='
					PRINT 'Transcation ID: ' + @TransactionID
					PRINT 'Transaction Date: ' + CAST(@TransactionDate AS VARCHAR)
					PRINT '================================='

						DECLARE CursorItem CURSOR
							FOR
								SELECT ItemName, Quantity, ItemPrice
								FROM MsItem mi JOIN DetailTransaction dt ON dt.ItemID = mi.ItemID JOIN MsItemBrand mb ON mb.ItemBrandID = mi.ItemBrandID JOIN MsItemCategory mc ON mc.ItemCategoryID = mc.ItemCategoryID
								WHERE @TransactionID = TransactionID

						OPEN CursorItem
							FETCH NEXT FROM CursorItem
							INTO @ItemName, @Quantity, @ItemPrice

							WHILE @@FETCH_STATUS=0
								BEGIN
								SET @Total1=@ItemPrice*@Quantity
								PRINT CAST(@Count AS VARCHAR)+'.'+@ItemName + '-' + CAST(@ItemPrice AS VARCHAR)
								PRINT 'Quantity : ' + CAST (@Quantity AS VARCHAR)
								PRINT 'Total :' + CAST(@Total1 AS VARCHAR)

								SET @Total += @Total1
								SET @Count= @Count + 1
								SET @Counter = @Counter -1

							FETCH NEXT FROM CursorItem
							INTO @ItemName, @Quantity, @ItemPrice
							END
							SET @Count=1
							PRINT 'Total Price : ' + CAST(@Total AS VARCHAR)
							
							CLOSE CursorItem
							
							DEALLOCATE CursorItem
				FETCH NEXT FROM transcursor
				INTO @TransactionID, @TransactionDate
				END

				CLOSE transcursor
				DEALLOCATE  transcursor

		END


		begin tran

		EXEC DisplayTransaction '10','11','2019'
		rollback

		
		DROP PROC DisplayTransaction