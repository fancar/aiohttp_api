
class DBhandler:

    def __init__(self,**kwargs):
        self.__dict__.update(kwargs)
        
    


    def add_and_commit(self,q):
        db.session.add(q)
        db.session.commit()