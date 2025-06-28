-- #################################################################
-- #           T-SQL Script to Create the Full ETL Job             #
-- #################################################################

USE msdb; -- Switch to the system database that manages jobs
GO

BEGIN TRANSACTION; -- Start a transaction to ensure all or nothing is created

DECLARE @ReturnCode INT = 0;
DECLARE @jobId BINARY(16);
DECLARE @job_owner sysname = SUSER_SNAME(); -- Use the current user as the owner

-- ====================================================================
-- Step 1: Create the Job
-- ====================================================================
-- This creates the main job container.
EXEC @ReturnCode =  msdb.dbo.sp_add_job
    @job_name = N'Daily_HR_ETL_Process', 
    @enabled = 1, 
    @notify_level_eventlog = 0, 
    @notify_level_email = 0, 
    @notify_level_netsend = 0, 
    @notify_level_page = 0, 
    @delete_level = 0, 
    @description = N'Runs the daily ETL process for the Human Resources Data Warehouse. It loads the staging area, updates dimensions, and updates fact tables.', 
    @category_name = N'[Uncategorized (Local)]', 
    @owner_login_name = @job_owner,
    @job_id = @jobId OUTPUT;

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback;

-- ====================================================================
-- Step 2: Add the Job Steps (The actions to be performed)
-- ====================================================================

-- === STEP 1: Load Staging Area ===
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep
    @job_id = @jobId,
    @step_name = N'1_Load_Staging_Area', 
    @step_id = 1, 
    @cmdexec_success_code = 0, 
    @on_success_action = 3, -- Go to the next step
    @on_fail_action = 2,    -- Quit the job reporting failure
    @retry_attempts = 0, 
    @retry_interval = 0, 
    @os_run_priority = 0,
    @subsystem = N'TSQL', 
    @command = N'EXEC StagingDB.HumanResources.usp_Load_All_HumanResources;', 
    @database_name = N'StagingDB';

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback;

-- === STEP 2: Update Dimensions ===
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep
    @job_id = @jobId,
    @step_name = N'2_Update_Dimensions', 
    @step_id = 2, 
    @cmdexec_success_code = 0, 
    @on_success_action = 3, -- Go to the next step
    @on_fail_action = 2,    -- Quit the job reporting failure
    @retry_attempts = 0, 
    @retry_interval = 0, 
    @os_run_priority = 0,
    @subsystem = N'TSQL', 
    @command = N'EXEC DataWarehouse.Dim.UpdateAllDimensions;', 
    @database_name = N'DataWarehouse';

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback;

-- === STEP 3: Update Facts ===
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep
    @job_id = @jobId,
    @step_name = N'3_Update_Facts', 
    @step_id = 3, 
    @cmdexec_success_code = 0, 
    @on_success_action = 1, -- Quit the job reporting success
    @on_fail_action = 2,    -- Quit the job reporting failure
    @retry_attempts = 0, 
    @retry_interval = 0, 
    @os_run_priority = 0,
    @subsystem = N'TSQL', 
    @command = N'EXEC DataWarehouse.Fact.UpdateAllFacts;', 
    @database_name = N'DataWarehouse';

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback;

-- ====================================================================
-- Step 3: Set the Job's Start Step and Server
-- ====================================================================
EXEC @ReturnCode = msdb.dbo.sp_update_job
    @job_id = @jobId,
    @start_step_id = 1;

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback;

EXEC @ReturnCode = msdb.dbo.sp_add_jobserver
    @job_id = @jobId,
    @server_name = N'(local)';

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback;

-- ====================================================================
-- Step 4: Create the Schedule for the Job
-- ====================================================================
-- This schedule runs the job daily at 2:00 AM.
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule
    @job_id = @jobId,
    @name = N'Daily_At_2AM', 
    @enabled = 1, 
    @freq_type = 4, -- Daily
    @freq_interval = 1, 
    @freq_subday_type = 1, 
    @freq_subday_interval = 0, 
    @freq_relative_interval = 0, 
    @freq_recurrence_factor = 0, 
    @active_start_date = 20240101, 
    @active_end_date = 99991231, 
    @active_start_time = 20000, -- 02:00:00
    @active_end_time = 235959;

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback;


COMMIT TRANSACTION;
GOTO EndSave;

QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION;

EndSave:
    PRINT 'Job "Daily_HR_ETL_Process" created successfully.';
GO
