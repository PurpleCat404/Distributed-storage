from distributed_storage import DistributedStorage

if __name__ == '__main__':

    storage = DistributedStorage('test.db')

    storage.initialize_db()

    storage.add_record("punkcake", "punkcakeovich")
    storage.add_record("maybe", "baby")


    print(storage.get_record("punkcake")) #punkcakeovich
    print(storage.get_record("maybe")) # baby (почему-то лишний отступ)
    print(storage.get_record("smth")) #None