from sqlalchemy.orm import Session

def get_or_create(db: Session, model, **kwargs):
    print("-"*80)
    print(kwargs)
    instance = db.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    else:
        instance = model(**kwargs)
        db.add(instance)
        db.commit()
        return instance
