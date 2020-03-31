#Each function return a valid SQL query which can be run on its own to look at snapshot of that data
#These queries assume the Innovare Production Schema as of 3/22/2020
#Adding a new function to this module will also add that query to the scorecard when scorecard.get_scorecard() is run
# Adding a new funciton to this module:
# Function names MUST be named exactly as the alias of the column you are returning from the query
# Each function can only reference one column alias
# This means that the results table of, for example, the dashboard column can only return the organization name, which is default,
# and a dashboard column

# This function returns an organization column,by default, and dashboard column displaying the number of dashboard that organization has
# function name [dashboards] matches returned column name [dashboards]
# In the query this is shown in the, COUNT(organization_charts.embed_url) AS dashboards, statement. Where 'AS dashboards' is the alias of the column
# this query will return and also the name of the function  

#return number of embed dashboards each org has  
def dashboards(dataset, tbl=""):
    query = f""" 
    SELECT organizations.name AS {tbl}organization, COUNT(organization_charts.embed_url) AS dashboards
    FROM 
       {dataset}.organizations LEFT JOIN
        {dataset}.organization_charts ON organization_charts.organization_id = organizations.id
        GROUP BY {tbl}organization
    """ 
    return query

#return the number of users who logged in once to the app
def login_percentage_once(dataset, tbl=""):
    query = f"""SELECT organizations.name AS {tbl}organization, ROUND(COUNT(CAST(users.last_login AS TIMESTAMP))/NULLIF(COUNT(CAST(users.organization_id AS INT64)),0),2 ) AS login_percentage_once
                FROM
                    {dataset}.organizations 
                    LEFT JOIN
                    {dataset}.users ON CAST(users.organization_id AS INT64) = organizations.id
                GROUP BY {tbl}organization"""
    return query

#returns the percentage of users who have logged in to the app in the last week rounded to two decimal places
def login_percentage_week(dataset, tbl=""):
    query = f""" 
            SELECT organizations.name AS {tbl}organization, ROUND(SUM(CASE WHEN CAST(users.last_login AS TIMESTAMP) > TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 7 DAY) THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN users.id IS NOT NULL THEN 1 ELSE 0 END), 0), 2) as login_percentage_week
          FROM 
            {dataset}.organizations LEFT JOIN
            {dataset}.users ON CAST(users.organization_id AS INT64) = organizations.id 
            GROUP BY {tbl}organization  
        """
    return query

#returns the percentage of users who have logged in to the app in the last month rounded to two decimal places
def login_percentage_month(dataset, tbl=""):
    query = f""" 
            SELECT organizations.name AS {tbl}organization, ROUND(SUM(CASE WHEN CAST(users.last_login AS TIMESTAMP) > TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 28 DAY) THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN users.id IS NOT NULL THEN 1 ELSE 0 END), 0),2) as login_percentage_month
          FROM 
            {dataset}.organizations LEFT JOIN
            {dataset}.users ON CAST(users.organization_id AS INT64) = organizations.id 
            GROUP BY {tbl}organization  
        """
    return query

#returns the total number of goals each organization has regardless of status 
def goals_number(dataset, tbl=""):
    query = f""" 
        SELECT organizations.name AS {tbl}organization, COUNT(goals.status_id) AS goals_number
        FROM 
            {dataset}.organizations 
            FULL JOIN
            {dataset}.goals ON goals.organization_id = organizations.id AND goals.status_id = 5
        GROUP BY {tbl}organization  
        """
    return query

#returns number of goals that also have a cycle
def goals_with_cycle(dataset, tbl=""):
    query= f""" 
        SELECT organizations.name AS {tbl}organization,IFNULL(COUNT(DISTINCT cycles.goal_id),0) AS goals_with_cycle
            FROM(
                {dataset}.goals 
                INNER JOIN 
                {dataset}.cycles ON cycles.goal_id = goals.id AND goals.status_id = 5
                FULL JOIN
                {dataset}.organizations ON organization_id = organizations.id
            )
            GROUP BY {tbl}organization
        """
    return query

#returns number of goals that also have a cycle AND action
def goals_with_cycle_actions(dataset, tbl=""):
    query= f""" 
            WITH cte AS (SELECT organizations.name AS {tbl}organization, goals.id AS goals, cycles.id AS cycle, actions.id AS actions 
                FROM(
                    {dataset}.actions 
                    INNER JOIN
                    {dataset}.cycles ON actions.cycle_id = cycles.id 
                    INNER JOIN 
                    {dataset}.goals ON cycles.goal_id = goals.id AND goals.status_id = 5
                    FULL JOIN
                    {dataset}.organizations ON goals.organization_id = organizations.id
                    )
                GROUP BY {tbl}organization, goals, cycle, actions)
                SELECT {tbl}organization, COUNT(DISTINCT goals) AS goals_with_cycle_actions
                FROM cte
                GROUP BY {tbl}organization
            """
    return query

#returns number of goals with the status of 'in progress'
def goal_in_progress(dataset, tbl=""):
    query=f""" 
       SELECT organizations.name AS {tbl}organization, COUNT(goals.status_id) AS goal_in_progress    
            FROM 
            {dataset}.goals 
            FULL JOIN
            {dataset}.organizations ON goals.organization_id = organizations.id AND goals.status_id = 5
            GROUP BY {tbl}organization
            """
    return query

#return goals thats that have reached their target w/closed cycles 
def goals_reached_percentage(dataset, tbl=""):
    query =f""" 
        WITH cte AS (SELECT  COUNT(DISTINCT goals.id) as goals_on_track, organizations.id
            FROM {dataset}.goals
            INNER JOIN
            {dataset}.metrics ON goals.metric_id = metrics.id  
            INNER JOIN
            {dataset}.cycles ON cycles.goal_id = goals.id AND cycles.status_id = 7
            INNER JOIN
            {dataset}.organizations ON goals.organization_id = organizations.id AND (cycles.progress >= goals.target AND metrics.results_rule = 2) OR (cycles.progress <= goals.target AND metrics.results_rule = 1)
            GROUP BY organizations.id)
        SELECT organizations.name AS {tbl}organization, COUNT(goals_on_track)/NULLIF(COUNT(goals.id),0) AS goals_reached_percentage
            FROM {dataset}.goals
            INNER JOIN
            {dataset}.cycles ON cycles.goal_id = goals.id AND cycles.status_id = 7
            FULL JOIN
            {dataset}.organizations ON goals.organization_id = organizations.id
            FULL JOIN
            cte ON cte.id = organizations.id
            GROUP BY {tbl}organization

            """
    return query

#returns number of cycles with the status of 'in progress'
def cycle_in_progress(dataset, tbl=""):
    query=f""" 
        SELECT organizations.name AS {tbl}organization, COUNT(DISTINCT cycles.goal_id) AS cycle_in_progress
            FROM {dataset}.cycles 
            LEFT JOIN
            {dataset}.goals ON cycles.goal_id = goals.id AND cycles.status_id=5 AND goals.status_id = 5 
            FULL JOIN
            {dataset}.organizations ON goals.organization_id = organizations.id 
            GROUP BY {tbl}organization
        """
    return query

#returns number of actions with the status of 'in progress'
def actions_in_progress(dataset, tbl=""):
    query=f""" 
       SELECT organizations.name AS {tbl}organization, COUNT(actions.status_id) AS actions_in_progress    
            FROM {dataset}.actions 
            INNER JOIN
            {dataset}.cycles ON actions.cycle_id = cycles.id AND actions.status_id = 2
            INNER JOIN
            {dataset}.goals ON cycles.goal_id = goals.id AND goals.status_id = 5
            FULL JOIN
            {dataset}.organizations ON goals.organization_id = organizations.id 
            GROUP BY {tbl}organization  
        """
    return query

#returns number of goals and cycles with the status of 'submitted'
def goals_cycles_submitted(dataset, tbl=""):
    query=f"""
        SELECT organizations.name AS {tbl}organization, COUNT(cycles.status_id) as goals_cycles_submitted
            FROM  {dataset}.cycles 
            LEFT JOIN
            {dataset}.goals ON cycles.goal_id = goals.id AND cycles.status_id=2 AND goals.status_id= 2
            FULL JOIN
            {dataset}.organizations ON goals.organization_id = organizations.id 
            GROUP BY {tbl}organization
        """
    return query

#returns number of goals with the status of 'closed'
def goals_closed(dataset, tbl=""):
    query=f""" 
       SELECT organizations.name AS {tbl}organization, COUNT(goals.status_id) AS goals_closed    
            FROM 
            {dataset}.goals 
            FULL JOIN
            {dataset}.organizations ON goals.organization_id = organizations.id AND goals.status_id = 7
            GROUP BY {tbl}organization
        """
    return query

#returns the number cycles with the status of 'closed'
def cycle_closed(dataset, tbl=""):
    query=f""" 
        SELECT organizations.name AS {tbl}organization, COUNT(cycles.status_id) AS cycle_closed
            FROM {dataset}.cycles 
            LEFT JOIN
            {dataset}.goals ON cycles.goal_id = goals.id AND cycles.status_id=7
            FULL JOIN
            {dataset}.organizations ON goals.organization_id = organizations.id 
            GROUP BY {tbl}organization
        """
    return query

#returns the number of actions that are in progress and past their end date 
def actions_off_track(dataset, tbl=""):
    query = f""" 
       SELECT organizations.name AS {tbl}organization, ROUND(SUM(CASE WHEN actions.end_date < CURRENT_TIMESTAMP then 1 else 0 END)/NULLIF(sum(CASE WHEN actions.start_date IS NOT NULL THEN 1 ELSE 0 end), 0),2)  as actions_off_track
            FROM {dataset}.actions
            INNER JOIN 
            {dataset}.cycles ON cycles.id = actions.cycle_id AND actions.status_id = 2
            INNER JOIN
            {dataset}.goals ON goals.id=cycles.goal_id 
            FULL JOIN 
            {dataset}.organizations ON organizations.id = goals.organization_id 
        GROUP BY {tbl}organization
        """
    return query

#returns percentage of milestones with the status of 'completed' rounded to 2 decimal places
def milestones_completed_percentage(dataset, tbl=""):
    query = f"""
        SELECT organizations.name AS {tbl}organization,  ROUND(SUM(CASE WHEN milestones.completed = 1 THEN 1 ELSE 0 END)/NULLIF(COUNT(milestones.completed),0),2)  AS milestones_completed_percentage
            FROM {dataset}.organizations
            LEFT JOIN 
            {dataset}.milestones ON milestones.organization_id = organizations.id
            GROUP BY {tbl}organization 
    """
    return query

#returns the title of the next milestone closest to today's date
def next_milestone_title(dataset, tbl=""):
    query=f"""
        WITH cte AS (SELECT organization_id AS oid, MIN(milestones.due_on) AS due
            FROM {dataset}.milestones
            WHERE milestones.due_on >= CURRENT_DATE AND milestones.completed = 0
            GROUP BY oid)
            SELECT organizations.name as {tbl}organization, STRING_AGG(milestones.title,', ') AS next_milestone_title
            FROM {dataset}.milestones
            INNER JOIN 
            cte ON cte.due = milestones.due_on AND milestones.organization_id = cte.oid AND milestones.completed = 0
            INNER JOIN 
            {dataset}.organizations ON organizations.id = cte.oid
            GROUP BY {tbl}organization
    """
    return query

#returns the date of the next milestone closest to today's date
def next_milestone_date(dataset, tbl=""):
    query=f"""
        SELECT organizations.name AS {tbl}organization, MIN(milestones.due_on) AS next_milestone_date
            FROM {dataset}.milestones
             FULL JOIN 
            {dataset}.organizations ON organizations.id = milestones.organization_id 
            WHERE milestones.due_on >= CURRENT_DATE AND milestones.completed = 0
            GROUP BY {tbl}organization
    """
    return query

#returns the location of organization as a state abbreviation
def location_state(dataset, tbl=""):
    query=f"""
        SELECT organizations.name AS {tbl}organization, regions.abbr AS location_state
            FROM {dataset}.regions
            LEFT JOIN
            {dataset}.addresses ON regions.id = addresses.region_id
            LEFT JOIN  
            {dataset}.organizations  ON addresses.id = organizations.address_id
            GROUP BY {tbl}organization, location_state
    """
    return query

 