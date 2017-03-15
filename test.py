from unittest import TestCase

from pyso import Model, TextField, IntegerField, create_table

Model.set_connection(":memory:")


class Post(Model):
    title = TextField()
    rating = IntegerField()


create_table(Post)


class ModTest(TestCase):
    def setUp(self):
        Post.create(title="hello", rating=1)
        Post.create(title="smith", rating=2)
        Post.create(title="mia mia", rating=3)
        Post.create(title="zzz zzz", rating=4)

    def tearDown(self):
        Post.all().delete()

    def test_new_model_w_save(self):
        length = Post.all().count()
        p = Post(title="a", rating=0)
        p.save()
        self.assertEqual(length + 1, Post.all().count())

    def test_new_model_wo_save(self):
        length = Post.all().count()
        Post(title="a", rating=0)
        self.assertEqual(length, Post.all().count())

    def test_new_model_w_create(self):
        length = Post.all().count()
        Post.create(title="a", rating=0)
        self.assertEqual(length + 1, Post.all().count())

    def test_operation_startswith_one_char(self):
        p = Post.filter(title__startswith="h")
        self.assertEqual(p.count(), 1)
        self.assertEqual(p[0].title, "hello")

    def test_operation_startswith_more_char(self):
        p = Post.filter(title__startswith="he")
        self.assertEqual(p.count(), 1)
        self.assertEqual(p[0].title, "hello")

    def test_operation_endswith_one_char(self):
        p = Post.filter(title__endswith="o")
        self.assertEqual(p.count(), 1)
        self.assertEqual(p[0].title, "hello")

    def test_operation_endswith_more_char(self):
        p = Post.filter(title__endswith="lo")
        self.assertEqual(p.count(), 1)
        self.assertEqual(p[0].title, "hello")

    def test_get_w_no_result(self):
        with self.assertRaises(Post.DoesNotExist):
            Post.get(title="hello2")

    def test_get_w_one_result(self):
        p = Post.get(title="hello")
        self.assertEqual(p.title, "hello")

    def test_get_w_more_result(self):
        with self.assertRaises(Post.MultipleObjectsReturned):
            Post.get(title__contains="i")
