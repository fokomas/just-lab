import unittest
import hello


class TestSumFunction(unittest.TestCase):
    def test_positive_numbers(self):
        self.assertEqual(hello.sum(3, 5), 8)

    def test_negative_numbers(self):
        self.assertEqual(hello.sum(-3, -5), -8)

    def test_mixed_numbers(self):
        self.assertEqual(hello.sum(3, -5), -2)

    def test_zero(self):
        self.assertEqual(hello.sum(0, 5), 5)
        self.assertEqual(hello.sum(0, 0), 0)


if __name__ == "__main__":
    unittest.main()
