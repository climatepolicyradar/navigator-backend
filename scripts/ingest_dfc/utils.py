from sqlalchemy.orm import Session


def get_or_create(db: Session, model, **kwargs):
    # Remove any extra kwargs before we do the search
    extra = {}
    if "extra" in kwargs.keys():
        extra = kwargs["extra"]
        del kwargs["extra"]
    instance = db.query(model).filter_by(**kwargs).first()

    if instance:
        return instance
    else:
        # Add the extra args in for creation
        for k, v in extra.items():
            kwargs[k] = v
        instance = model(**kwargs)
        db.add(instance)
        db.commit()
        return instance

def to_dict(base_object: object):
    extra = ["__class__"]
    return dict((col, str(getattr(base_object, col))) for col in base_object.__table__.columns.keys() + extra)

