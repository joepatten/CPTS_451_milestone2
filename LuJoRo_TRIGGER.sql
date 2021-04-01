-- 5.a
create Trigger TipUpdate
    After Insert On Tip
    For Tip.business_id = Buisiness.business_id
        Set (num_tips = num_tips + 1) 
    For Tip.user_id = UserTable.user_id
        Set (tip_count = tip_count + 1)

--5.b
create Trigger CheckInUpdate
    After Insert On CheckIn
    For CheckIn.business_id = Buisiness.business_id
        Set (num_checkins = num_checkins + 1)

--5.c
create Trigger TotalLikeUser
    After Update on Tip.likes
    For Tip.user_id = UserTable.user_id
        Set (total_likes = total_likes + 1)
