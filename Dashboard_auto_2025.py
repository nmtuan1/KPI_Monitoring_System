import pandas as pd
from datetime import datetime
import numpy as np

df_resolved = pd.read_excel('C:/.../../..your direcotry/ABCD_Resolved_Auto_collect.xlsx')
df_bugfound = pd.read_excel('C:/.../../..your direcotry/ABCD_Bug_found_Auto_collect.xlsx')
df_PR_TMA = pd.read_excel('C:/.../../..your direcotry/ABCD_PR_Auto_collect.xlsx')


############ TICKET RESOLVED ######################

# Drop unnecessary colum 
#df_resolved=df_resolved.drop(['Project type', 'Project lead', 'Project description', 'Project url'], axis=1)

#Not count Epic
df_resolved=df_resolved[(df_resolved['Issue Type']!='Epic')]

## Tickets Resolved analysis

#Convert Created & Resolved date to datetime datatype
df_resolved['Created']=pd.to_datetime(df_resolved['Created'])
df_resolved['Resolved']=pd.to_datetime(df_resolved['Resolved'])


#Convert Story point to int
df_resolved['Story Points']=df_resolved['Story Points'].fillna(0)
df_resolved['Story Points']=df_resolved['Story Points'].astype(int)
#Convert Story point 'QE Automated TCs' to int
df_resolved['Automated TC']=df_resolved['Automated TC'].fillna(0)
df_resolved['Automated TC']=df_resolved['Automated TC'].astype(int)
#Convert Story point 'Custom field (Need)' to int
df_resolved['Manual Executed']=df_resolved['Manual Executed'].fillna(0)
df_resolved['Manual Executed']=df_resolved['Manual Executed'].astype(int)


#Add Column User
#df_resolved['User']=df_resolved['Assignee'].str.split('@').str[0]
df_resolved['User']=df_resolved['Assignee']


#Define TEAM
df_resolved['Team']='None'
df_resolved.loc[
    (df_resolved['Assignee']=='Tuan Nguyen 1')|(df_resolved['Assignee']=='Huynh Hoang'), 'Team'
    ]='CIM'
df_resolved.loc[
    (df_resolved['Assignee']=='Dang Chau')|(df_resolved['Assignee']=='Nhi Tran')|
    (df_resolved['Assignee']=='Minh Trinh Thi'), 'Team'
    ]='Performance'  
df_resolved.loc[(df_resolved['Assignee']=='Khoa Phan')|(df_resolved['Assignee']=='Y Truong'), 'Team']='SDET'
df_resolved.loc[
    (df_resolved['Assignee']=='Cong Tran')|(df_resolved['Assignee']=='Thien Bui')|
    (df_resolved['Assignee']=='Giau Vo Ngoc')|(df_resolved['Assignee']=='Nga Ung'), 'Team'
    ]='Automation'
df_resolved.loc[
    (df_resolved['Assignee']=='Tuan Le')|(df_resolved['Assignee_user']=='hieu.doung@abc.com')|
    (df_resolved['Assignee_user']=='thien.le@abc.com'), 'Team'
    ]='OVA'
df_resolved.loc[
    (df_resolved['Assignee']=='Trang Nguyen')|(df_resolved['Assignee']=='Vinh Le')|
    (df_resolved['Assignee']=='Toan Nguyen')|(df_resolved['Assignee']=='Phuong Nguyen')|
    (df_resolved['Assignee']=='Liem Nguyen')|(df_resolved['Assignee']=='Ngan Hua')|
    (df_resolved['Assignee_user']=='trinh.nguyen@abc.com')|(df_resolved['Assignee_user']=='yen.le@abc.com'), 'Team'
    ]='Regression'
df_resolved.loc[
    (df_resolved['Assignee_user']=='hau.do@abc.com')|(df_resolved['Assignee_user']=='chuong.nguyen@abc.com')|
    (df_resolved['Assignee_user']=='trinh.nguyen2@abc.com')|(df_resolved['Assignee_user']=='ai.ngo@abc.com')|
    (df_resolved['Assignee_user']=='bao.mang@abc.com')|(df_resolved['Assignee_user']=='duc.nguyen@abc.com')|
    (df_resolved['Assignee_user']=='thao.dang@abc.com')|(df_resolved['Assignee_user']=='tram.le@abc.com')|
    (df_resolved['Assignee_user']=='trang.vo@abc.com')|(df_resolved['Assignee_user']=='tu.le@abc.com')|
    (df_resolved['Assignee_user']=='tram.nguyen@abc.com')|(df_resolved['Assignee_user']=='dat.pham@abc.com')|
    (df_resolved['Assignee_user']=='hung.nguyen@abc.com')|(df_resolved['Assignee_user']=='duc.pham@abc.com')|
    (df_resolved['Assignee_user']=='hau.van@abc.com'), 'Team'
    ]='Feature'

# Add column Time series
df_resolved['Month']=df_resolved['Resolved'].dt.month
df_resolved['Week'] = df_resolved['Resolved'].dt.isocalendar().week
df_resolved['day-name']=df_resolved['Resolved'].dt.day_name()
df_resolved['Day order']='0'
df_resolved.loc[(df_resolved['day-name']=='Monday'), 'Day order']='1'
df_resolved.loc[(df_resolved['day-name']=='Tuesday'), 'Day order']='2'
df_resolved.loc[(df_resolved['day-name']=='Wednesday'), 'Day order']='3'
df_resolved.loc[(df_resolved['day-name']=='Thursday'), 'Day order']='4'
df_resolved.loc[(df_resolved['day-name']=='Friday'), 'Day order']='5'
df_resolved.loc[(df_resolved['day-name']=='Saturday'), 'Day order']='6'
df_resolved.loc[(df_resolved['day-name']=='Sunday'), 'Day order']='7'

#Create new Table Team size
df_team_size=df_resolved.groupby('Team').agg(
    total_member=('Assignee','nunique')
).reset_index().sort_values(by='total_member', ascending=False)




# Group Issue Type
df_resolved_IssueType=df_resolved[(df_resolved['Resolution']=='Done') | (df_resolved['Resolution']=='Fixed')].groupby(['Issue Type','User','Team','Week']).agg(
    total_Ticket =('Issue key','count'),
).reset_index().sort_values(by='Week', ascending=False)

# Create DF Resolved filter
df_resolved_filter=df_resolved[(df_resolved['Resolution']=='Done') | (df_resolved['Resolution']=='Fixed')].groupby(['Team','User','Week', 'Month']).agg(
    total_Ticket =('Issue key','count'),
    total_Storypoint =('Story Points','sum'),
    total_Automated = ('Automated TC','sum'),
    total_Manual = ('Manual Executed','sum')
).reset_index().sort_values(by=['Team','total_Storypoint'], ascending =[True, False])

# DF Resolved with issue type
# Group Issue Type
df_resolved_IssueType=df_resolved[(df_resolved['Resolution']=='Done') | (df_resolved['Resolution']=='Fixed')].groupby(['Issue Type','User','Team','Week']).agg(
    total_Ticket =('Issue key','count'),
).reset_index().sort_values(by='Week', ascending=False)


############ Bug Found analysis 2025 ######################
#Add Created Month and week colum
df_bugfound['Created']=pd.to_datetime(df_bugfound['Created'])

df_bugfound['Month']= df_bugfound['Created'].dt.month
df_bugfound['day-name']=df_bugfound['Created'].dt.day_name()
df_bugfound['Day order']='0'
df_bugfound.loc[(df_bugfound['day-name']=='Monday'), 'Day order']='1'
df_bugfound.loc[(df_bugfound['day-name']=='Tuesday'), 'Day order']='2'
df_bugfound.loc[(df_bugfound['day-name']=='Wednesday'), 'Day order']='3'
df_bugfound.loc[(df_bugfound['day-name']=='Thursday'), 'Day order']='4'
df_bugfound.loc[(df_bugfound['day-name']=='Friday'), 'Day order']='5'
df_bugfound.loc[(df_bugfound['day-name']=='Saturday'), 'Day order']='6'
df_bugfound.loc[(df_bugfound['day-name']=='Sunday'), 'Day order']='7'

df_bugfound['Week'] = df_bugfound['Created'].dt.isocalendar().week

#Add Colum User
#df_bugfound['User']=df_bugfound['Reporter'].str.split('@').str[0]
df_bugfound['User']=df_bugfound['Reporter']

#Define TEAM
df_bugfound['Team']='None'
df_bugfound.loc[(df_bugfound['Reporter_user']=='tuan.nguyen1@abc.com')|(df_bugfound['Reporter_user']=='huynh.hoang@abc.com'), 'Team']='CIM'
df_bugfound.loc[
    (df_bugfound['Reporter_user']=='dang.chau@abc.com')|(df_bugfound['Reporter_user']=='nhi.tran@abc.com')|
    (df_bugfound['Reporter_user']=='minh.trinhthi@abc.com'), 'Team'
    ]='Performance'
df_bugfound.loc[(df_bugfound['Reporter_user']=='khoa.phan@abc.com')|(df_bugfound['Reporter_user']=='y.truong@abc.com'), 'Team']='SDET'
df_bugfound.loc[
    (df_bugfound['Reporter_user']=='cong.tran@abc.com')|(df_bugfound['Reporter_user']=='thien.bui@abc.com')|
    (df_bugfound['Reporter_user']=='giau.vongoc@abc.com')|(df_bugfound['Reporter_user']=='nga.ung@abc.com'), 'Team']='Automation'
df_bugfound.loc[
    (df_bugfound['Reporter_user']=='tuan.le@abc.com')|(df_bugfound['Reporter_user']=='hieu.doung@abc.com')|
    (df_bugfound['Reporter_user']=='thien.le@abc.com'), 'Team']='OVA'
df_bugfound.loc[
    (df_bugfound['Reporter_user']=='trang.nguyen@abc.com')|(df_bugfound['Reporter_user']=='vinh.le@abc.com')|
    (df_bugfound['Reporter_user']=='toan.nguyen@abc.com')|(df_bugfound['Reporter_user']=='phuong.nguyen@abc.com')|
    (df_bugfound['Reporter_user']=='liem.nguyen@abc.com')|(df_bugfound['Reporter_user']=='ngan.hua@abc.com')|
    (df_bugfound['Reporter_user']=='trinh.nguyen@abc.com')|(df_bugfound['Reporter_user']=='yen.le@abc.com'), 'Team']='Regression'
df_bugfound.loc[
    (df_bugfound['Reporter_user']=='hau.do@abc.com')|(df_bugfound['Reporter_user']=='chuong.nguyen@abc.com')|
    (df_bugfound['Reporter_user']=='trinh.nguyen2@abc.com')|(df_bugfound['Reporter_user']=='ai.ngo@abc.com')|
    (df_bugfound['Reporter_user']=='bao.mang@abc.com')|(df_bugfound['Reporter_user']=='duc.nguyen@abc.com')|
    (df_bugfound['Reporter_user']=='thao.dang@abc.com')|(df_bugfound['Reporter_user']=='tram.le@abc.com')|
    (df_bugfound['Reporter_user']=='trang.vo@abc.com')|(df_bugfound['Reporter_user']=='tu.le@abc.com')|
    (df_bugfound['Reporter_user']=='tram.nguyen@abc.com')|(df_bugfound['Reporter_user']=='dat.pham@abc.com')|
    (df_bugfound['Reporter_user']=='hung.nguyen@abc.com')|(df_bugfound['Reporter_user']=='hau.van@abc.com')|(df_bugfound['Reporter_user']=='duc.pham@abc.com'), 'Team'
    ]='Feature'
    

#df_bugfound_Team_User=df_bugfound[(df_bugfound['Resolution']!='Duplicate')| (df_bugfound['Resolution']!='Invalid')].groupby(['User','Team']).agg(
#    Total_Bugfound=('Issue id','count')
#).reset_index().sort_values(by=['Team','Total_Bugfound'], ascending=[True, False])

# Add DF bug found filter
df_bugfound_filter=df_bugfound[(df_bugfound['Resolution']!='Invalid')].groupby(['Team','User','Week', 'Month']).agg(
    total_Bug =('Issue key','count'),
).reset_index().sort_values(by=['Week','User'], ascending =[True, False])

# Merge DF Ticket Resolved filter and Bug found Filter
df_join_Resolved_and_Bug=pd.merge(df_resolved_filter, df_bugfound_filter, how='outer', on=['Team','User','Week', 'Month'])
df_join_Resolved_and_Bug=df_join_Resolved_and_Bug.fillna(0)

# Insert column 'Bug ratio'
df_join_Resolved_and_Bug['Bug ratio'] = np.where(
    df_join_Resolved_and_Bug['total_Manual'] != 0,
    df_join_Resolved_and_Bug['total_Bug'] / df_join_Resolved_and_Bug['total_Manual'],
    df_join_Resolved_and_Bug['total_Bug'] / 1
)

# Create new table 'Bug ratio' by team/week 
df_bug_ratio=df_join_Resolved_and_Bug.groupby(['Team','Week']).agg(
    total_Bug =('total_Bug','sum'),
    total_Manual =('total_Manual','sum')
).reset_index().sort_values(by=['Team','Week'], ascending =[True, True])

df_bug_ratio['Bug ratio'] = np.where(
    df_bug_ratio['total_Manual'] != 0,
    df_bug_ratio['total_Bug'] / df_bug_ratio['total_Manual'],
    df_bug_ratio['total_Bug'] / 1
)


#df_join_Resolved_and_Bug['baseline']='50'
#df_join_Resolved_and_Bug['total_Bug'].fillna(0, inplace=True)

#df_join_Resolved_and_Bug.loc[df_join_Resolved_and_Bug['Team'] == 'Automation', ['Baseline ticket', 'Baseline Storypoint', 'Baseline Manual']] = [3, 9, 0]
#df_join_Resolved_and_Bug.loc[df_join_Resolved_and_Bug['Team'] == 'SDET', ['Baseline ticket', 'Baseline Storypoint', 'Baseline Manual']] = [2, 6, 0]
#df_join_Resolved_and_Bug.loc[df_join_Resolved_and_Bug['Team'] == 'Performance', ['Baseline ticket', 'Baseline Storypoint', 'Baseline Manual']] = [2, 6, 0]
#df_join_Resolved_and_Bug.loc[df_join_Resolved_and_Bug['Team'] == 'CIM', ['Baseline ticket', 'Baseline Storypoint', 'Baseline Manual']] = [1, 3, 0]
#df_join_Resolved_and_Bug.loc[df_join_Resolved_and_Bug['Team'] == 'Feature', ['Baseline ticket', 'Baseline Storypoint', 'Baseline Manual']] = [13, 39, 0]
#df_join_Resolved_and_Bug.loc[df_join_Resolved_and_Bug['Team'] == 'Regression', ['Baseline ticket', 'Baseline Storypoint', 'Baseline Manual']] = [40, 40, 0]
#df_join_Resolved_and_Bug.loc[df_join_Resolved_and_Bug['Team'] == 'OVA', ['Baseline ticket', 'Baseline Storypoint', 'Baseline Manual']] = [3, 9, 0]





############ PR MenloQA  ################

#Convert Opened & Merged date to datetime datatype
df_PR_TMA['Created At']=pd.to_datetime(df_PR_TMA['Created At'])
df_PR_TMA['Closed At']=pd.to_datetime(df_PR_TMA['Closed At'])

#Convert Additions to int
df_PR_TMA['Comments']=df_PR_TMA['Comments'].fillna(0)
df_PR_TMA['Additions']=df_PR_TMA['Additions'].astype(int)
#Convert Deletions to int
df_PR_TMA['Deletions']=df_PR_TMA['Deletions'].fillna(0)
df_PR_TMA['Deletions']=df_PR_TMA['Deletions'].astype(int)
#Convert Approvals to int
df_PR_TMA['Approvals']=df_PR_TMA['Approvals'].fillna(0)
df_PR_TMA['Approvals']=df_PR_TMA['Approvals'].astype(int)
#Convert Comments to int
df_PR_TMA['Comments']=df_PR_TMA['Comments'].fillna(0)
df_PR_TMA['Comments']=df_PR_TMA['Comments'].astype(int)

#Define TEAM
df_PR_TMA['Team']='None'
df_PR_TMA.loc[(df_PR_TMA['Creator']=='Tuan Nguyen')|(df_PR_TMA['Creator']=='Huynh Hoang'), 'Team']='CIM'
df_PR_TMA.loc[(df_PR_TMA['Creator']=='Dang Chau')|(df_PR_TMA['Creator']=='Nhi Tran')|(df_PR_TMA['Creator']=='Minh Trinh Thi'), 'Team']='Performance'
df_PR_TMA.loc[(df_PR_TMA['Creator']=='Khoa Phan')|(df_PR_TMA['Creator']=='Y Truong'), 'Team']='SDET'
df_PR_TMA.loc[
    (df_PR_TMA['Creator']=='Cong Tran')|(df_PR_TMA['Creator']=='Thien Bui')|
    (df_PR_TMA['Creator']=='Giau Vo Ngoc')|(df_PR_TMA['Creator']=='Nga Ung'), 'Team'
    ]='Automation'
df_PR_TMA.loc[
    (df_PR_TMA['Creator']=='Tuan Le')|(df_PR_TMA['Username']=='hieu.doung@abc.com')|
    (df_PR_TMA['Username']=='thien.le@abc.com'), 'Team'
    ]='OVA'
df_PR_TMA.loc[
    (df_PR_TMA['Creator']=='Trang Nguyen')|(df_PR_TMA['Creator']=='Vinh Le')|
    (df_PR_TMA['Creator']=='Toan Nguyen')|(df_PR_TMA['Creator']=='Phuong Nguyen')|
    (df_PR_TMA['Creator']=='Liem Nguyen')|(df_PR_TMA['Creator']=='Ngan Hua')|
    (df_PR_TMA['Username']=='trinh.nguyen@abc.com')|(df_PR_TMA['Creator']=='Yen Le'), 'Team'
    ]='Regression'
df_PR_TMA.loc[
    (df_PR_TMA['Creator']=='Hau Do')|(df_PR_TMA['Creator']=='Chuong Nguyen')|
    (df_PR_TMA['Username']=='trinh.nguyen2@abc.com')|(df_PR_TMA['Creator']=='Ai Ngo')|
    (df_PR_TMA['Creator']=='Bao Mang')|(df_PR_TMA['Creator']=='Duc Nguyen')|
    (df_PR_TMA['Creator']=='Thao Dang')|(df_PR_TMA['Creator']=='Tram Le')|
    (df_PR_TMA['Creator']=='Trang Vo')|(df_PR_TMA['Creator']=='Tu Le')|
    (df_PR_TMA['Creator']=='Tram Nguyen')|(df_PR_TMA['Creator']=='Dat Pham')|
    (df_PR_TMA['Creator']=='Hung Nguyen')|(df_PR_TMA['Creator']=='Duc Pham')|(df_PR_TMA['Creator']=='Hau Van'), 'Team'
    ]='Feature'
    
# Add column User
df_PR_TMA['User']=df_PR_TMA['Creator']

# Add column Time series
df_PR_TMA['Month']=df_PR_TMA['Created At'].dt.month
df_PR_TMA['Week'] = df_PR_TMA['Created At'].dt.isocalendar().week
df_PR_TMA['day-name']=df_PR_TMA['Created At'].dt.day_name()
df_PR_TMA['Day order']='0'
df_PR_TMA.loc[(df_PR_TMA['day-name']=='Monday'), 'Day order']='1'
df_PR_TMA.loc[(df_PR_TMA['day-name']=='Tuesday'), 'Day order']='2'
df_PR_TMA.loc[(df_PR_TMA['day-name']=='Wednesday'), 'Day order']='3'
df_PR_TMA.loc[(df_PR_TMA['day-name']=='Thursday'), 'Day order']='4'
df_PR_TMA.loc[(df_PR_TMA['day-name']=='Friday'), 'Day order']='5'
df_PR_TMA.loc[(df_PR_TMA['day-name']=='Saturday'), 'Day order']='6'
df_PR_TMA.loc[(df_PR_TMA['day-name']=='Sunday'), 'Day order']='7'


df_PR_TMA['Code_added']=df_PR_TMA['Additions']-df_PR_TMA['Deletions']


# Create DF Resolved filter
df_PR_TMA_filter=df_PR_TMA.groupby(['Team','User','Week', 'Month']).agg(
    total_PR = ('ID','count')
).reset_index().sort_values(by=['Team','total_PR'], ascending =[True, False])

# Merge DF Ticket Resolved filter and Bug found Filter
df_join_Resolved_Bug_PR=pd.merge(df_join_Resolved_and_Bug, df_PR_TMA_filter, how='outer', on=['Team','User','Week', 'Month'])
df_join_Resolved_Bug_PR=df_join_Resolved_Bug_PR.fillna(0)



# Create DF alarm for PR cycletime > 5
df_PR_TMA_ALARM = df_PR_TMA[(df_PR_TMA['State'] == 'OPEN') & (df_PR_TMA['Cycle Time'] > 5)]


############ Check in gate 2024 ######################

# Tạo danh sách các ngày trong tuần
days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
day_orders = list(range(1, 8))

# Tạo DataFrame
df_access_day_order = pd.DataFrame({'Day order': day_orders, 'day-name': days_of_week})

# Convert datatype of Date Tracking to Datetime
df_access['Date Tracking'] = pd.to_datetime(df_access['Date Tracking'])


# Define team
df_access['Team']='None'
df_access.loc[(df_access['Username']=='nmtuan1')|(df_access['Username']=='hthuynh'), 'Team']='CIM'
df_access.loc[(df_access['Username']=='dbchau')|(df_access['Username']=='ttnhi')|(df_access['Username']=='tthiminh'), 'Team']='Performance'
df_access.loc[(df_access['Username']=='	pakhoa')|(df_access['Username']=='tty'), 'Team']='SDET'
df_access.loc[
    (df_access['Username']=='tncong')|(df_access['Username']=='pgduc')|
    (df_access['Username']=='vngiau'), 'Team']='Automation'
df_access.loc[
    (df_access['Username']=='lctuan')|(df_access['Username']=='dmhieu1')|
    (df_access['Username']=='ltsthien'), 'Team']='OVA'
df_access.loc[
    (df_access['Username']=='nkktrang')|(df_access['Username']=='lqtvinh')|
    (df_access['Username']=='ntantoan')|(df_access['Username']=='nlephuong')|
    (df_access['Username']=='ntmliem')|(df_access['Username']=='hbkngan')|
    (df_access['Username']=='nthithaotrinh')|(df_access['Username']=='lhyen'), 'Team']='Regression'
df_access.loc[
    (df_access['Username']=='dminhhau')|(df_access['Username']=='nhuuchuong')|
    (df_access['Username']=='nnptrinh')|(df_access['Username']=='nltai')|(df_access['Username']=='naduc')|
    (df_access['Username']=='dhsthao')|(df_access['Username']=='ltntram1')|
    (df_access['Username']=='vtttrang')|(df_access['Username']=='lctu')|
    (df_access['Username']=='ntqtram')|(df_access['Username']=='pnadat')|
    (df_access['Username']=='nduchung'), 'Team']='Feature'
    
# Add column Time series
df_access['Month']=df_access['Date Tracking'].dt.month
df_access['Week'] = df_access['Date Tracking'].dt.isocalendar().week
df_access['day-name']=df_access['Date Tracking'].dt.day_name()
df_access['Day order']='0'
df_access.loc[(df_access['day-name']=='Monday'), 'Day order']='1'
df_access.loc[(df_access['day-name']=='Tuesday'), 'Day order']='2'
df_access.loc[(df_access['day-name']=='Wednesday'), 'Day order']='3'
df_access.loc[(df_access['day-name']=='Thursday'), 'Day order']='4'
df_access.loc[(df_access['day-name']=='Friday'), 'Day order']='5'
df_access.loc[(df_access['day-name']=='Saturday'), 'Day order']='6'
df_access.loc[(df_access['day-name']=='Sunday'), 'Day order']='7'

# Add column 'Access Late' and 'Not Access'
df_access['Access Late']='0'
df_access['Not Access']='0'
df_access.loc[(df_access['Check-In Time']!='Not Access'),'Access Late']='1'
df_access.loc[(df_access['Check-In Time']=='Not Access'),'Not Access']='1'

# Add cột 'Checked-In'
df_access['Checked-In']=df_access[(df_access['Check-In Time']!='Not Access')]['Check-In Time']
df_access['Late in 9:00-9:15AM']=0
df_access['Late in 9:15-9:30AM']=0
df_access['Late in 9:30-10:00AM']=0
df_access['Late in 10:00-10:30AM']=0


# Chuyển đổi cột 'Check-In Time' thành kiểu datetime
df_access['Checked-In'] = pd.to_datetime(df_access['Checked-In'], format='mixed')

# Hàm để kiểm tra và cập nhật giá trị trong cột "Late in"
def update_late_status(row):
    check_in_time = row['Checked-In']

    if pd.Timestamp('09:00:00') <= check_in_time < pd.Timestamp('09:15:00'):
        row['Late in 9:00-9:15AM'] = 1
    elif pd.Timestamp('09:16:00') <= check_in_time < pd.Timestamp('09:30:00'):
        row['Late in 9:15-9:30AM'] = 1
    elif pd.Timestamp('09:31:00') <= check_in_time < pd.Timestamp('10:00:00'):
        row['Late in 9:30-10:00AM'] = 1
    elif pd.Timestamp('10:01:00') <= check_in_time < pd.Timestamp('10:30:00'):
        row['Late in 10:00-10:30AM'] = 1
    return row

# Áp dụng hàm cho từng hàng trong DataFrame
df_access = df_access.apply(update_late_status, axis=1)

#Fileter MenloQA team
df_access_MenloQA=df_access[(df_access['Project']=='Menlo QA')]
df_access_late_MenloQA=df_access[(df_access['Project']=='Menlo QA')].loc[(df_access['Check-In Time']!='Not Access') & (pd.notnull(df_access['Check-In Time']))]
df_access_Noaccess_MenloQA=df_access[(df_access['Project']=='Menlo QA')&(df_access['Check-In Time']=='Not Access')]