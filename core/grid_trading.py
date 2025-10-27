import pandas as pd
from datetime import datetime


class GridTrading:
    def __init__(
        self,
        symbol,
        initial_price,
        grid_spacing_pct,
        fee_rate=0.001,
        entry_alloc=1,
        num_grids=10,
        budget=1000,
    ):
        self.symbol = symbol
        self.initial_price = initial_price
        self.grid_spacing_pct = grid_spacing_pct
        self.num_grids = num_grids
        self.budget = budget
        self.fee_rate = fee_rate

        # Calculate position size per grid
        self.position_size = budget * entry_alloc

        # Generate grid levels
        self.grid_levels = self._calculate_grid_levels()

        # Initialize trading state
        self.cash = budget
        self.positions = {}
        self.total_coin = 0.0
        self.realized_profit = 0.0
        self.total_fee = 0.0  # 🧾 total biaya fee kumulatif

        print("\n=== Grid Trading Setup Details ===")
        print(f"Symbol: {self.symbol}")
        print(f"Initial Price: ${self.initial_price:,.2f}")
        print(f"Total Budget: ${self.budget:,.2f}")
        print(f"Position Size per Grid: ${self.position_size:,.2f}")

    def _calculate_grid_levels(self):
        """
        Calculate grid trading levels below the initial price based on specified spacing.
        Returns:
            list: List of price levels in descending order for grid trading.
        """
        if not isinstance(self.initial_price, (int, float)) or self.initial_price <= 0:
            raise ValueError("Initial price must be a positive number")

        if (
            not isinstance(self.grid_spacing_pct, (int, float))
            or self.grid_spacing_pct <= 0
        ):
            raise ValueError("Grid spacing percentage must be a positive number")

        if not isinstance(self.num_grids, int) or self.num_grids <= 0:
            raise ValueError("Number of grids must be a positive integer")

        # Calculate grid levels with input validation
        levels = []
        total_discount = 0

        for i in range(1, self.num_grids + 1):
            # Calculate discount for current level
            total_discount = i * self.grid_spacing_pct / 100

            # Ensure discount doesn't exceed 100%
            if total_discount >= 1:
                break

            # Calculate price level
            level = self.initial_price * (1 - total_discount)
            levels.append(round(level, 2))

        return levels

    def _apply_fee(self, amount):
        return amount * (1 - self.fee_rate)

    def display_grid_setup(self):

        print("\nGrid Levels:")
        print("-" * 70)
        print(
            f"{'Level':^6} | {'Price':^15} | {'Discount %':^10} | {'USD Amount':^12} | {'Coin Amount':^12}"
        )
        print("-" * 70)

        for i, level in enumerate(self.grid_levels):
            discount = ((self.initial_price - level) / self.initial_price) * 100
            coin_amount = self.position_size / level

            print(
                f"{i+1:^6} | ${level:>13,.2f} | {discount:>9.2f}% | ${self.position_size:>10,.2f} | {coin_amount:>11.5f}"
            )

        print("-" * 70)
        print(f"Total Grids: {len(self.grid_levels)}")
        print(
            f"Grid Range: {self.grid_spacing_pct * self.num_grids:.1f}% below initial price"
        )

    def execute_buy_order(self, current_price, verbose=True):
        executed_orders = []

        for level in self.grid_levels:
            # Check jika price tersentuh sesuai level dan belum punya position
            if current_price <= level and level not in self.positions:
                # Check if had enough cash
                if self.cash >= self.position_size:
                    fee = self.position_size * self.fee_rate
                    quantity = self._apply_fee(
                        self.position_size / level
                    )  # fee diterapkan ke quantity yang diterima

                    # Execute Buy
                    self.positions[level] = {
                        "quantity": quantity,
                        "buy_price": level,
                        "timestamp": datetime.now(),
                    }

                    self.cash -= self.position_size
                    self.total_coin += quantity
                    self.total_fee += fee  # 🧾 catat fee

                    executed_orders.append(
                        {
                            "type": "BUY",
                            "price": level,
                            "quantity": quantity,
                            "amount": self.position_size,
                            "fee": fee,
                        }
                    )

                    if verbose:
                        print(f"[BUY] {quantity:.5f} {self.symbol} at ${level:,.2f}")

        return executed_orders

    def execute_sell_order(self, current_price, profit_target_pct=3.4, verbose=True):
        executed_orders = []

        # Check semua active positions
        for level, position in list(self.positions.items()):
            target_price = position["buy_price"] * (1 + (profit_target_pct / 100))

            # Sell jika current_price >= target price
            if current_price >= target_price:
                sell_amount = position["quantity"] * current_price
                sell_amount_net = self._apply_fee(
                    sell_amount
                )  # fee diterapkan ke hasil jual
                fee = sell_amount - sell_amount_net
                profit = sell_amount_net - self.position_size

                # Execute Sell
                self.cash += sell_amount_net  # 💡 cash sudah dikurangi fee
                self.total_coin -= position["quantity"]
                self.realized_profit += profit
                self.total_fee += fee  # 🧾 catat fee

                executed_orders.append(
                    {
                        "type": "SELL",
                        "buy_price": position["buy_price"],
                        "sell_price": current_price,
                        "quantity": position["quantity"],
                        "profit": profit,
                        "fee": fee,
                    }
                )

                if verbose:
                    print(
                        f"[SELL] {position['quantity']:.5f} {self.symbol} at ${current_price:,.2f} | Profit: ${profit:,.2f}"
                    )

                del self.positions[level]

        return executed_orders

    def simulate_grid_trading(self, price_data, profit_target_pct):
        print(
            f"Simulating Grid Trading for {self.symbol} with TP : {profit_target_pct}%"
        )
        trading_log, portofolio_history = [], []

        for date, row in price_data.iterrows():
            current_price = float(row["Close"])
            # print(f'Ini harga sekarang : {current_price:,.2f}')

            # Execute trading logic
            buy_orders = self.execute_buy_order(current_price)
            sell_orders = self.execute_sell_order(current_price, profit_target_pct)

            # Log all trades
            for order in buy_orders + sell_orders:
                order["date"] = date
                trading_log.append(order)

            # Record portfolio state
            crypto_value = self.total_coin * current_price
            total_value = self.cash + crypto_value

            portofolio_history.append(
                {
                    "date": date,
                    "price": current_price,
                    "cash": self.cash,
                    "crypto_value": crypto_value,
                    "total_value": total_value,
                    "realized_profit": self.realized_profit,
                    "total_fee": self.total_fee,
                    "net_profit_after_fee": self.realized_profit - self.total_fee,
                }
            )

        print(f"\n✅ Total Fee Dikeluarkan: ${self.total_fee:,.2f}")
        return pd.DataFrame(trading_log), pd.DataFrame(portofolio_history)
