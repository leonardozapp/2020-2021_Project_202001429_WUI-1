<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\IndicadoresCarteira $indicadoresCarteira
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('Edit Indicadores Carteira'), ['action' => 'edit', $indicadoresCarteira->carteiras_investimento_id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Indicadores Carteira'), ['action' => 'delete', $indicadoresCarteira->carteiras_investimento_id], ['confirm' => __('Are you sure you want to delete # {0}?', $indicadoresCarteira->carteiras_investimento_id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Indicadores Carteiras'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Indicadores Carteira'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="indicadoresCarteiras view content">
            <h3><?= h($indicadoresCarteira->carteiras_investimento_id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Carteiras Investimento') ?></th>
                    <td><?= $indicadoresCarteira->has('carteiras_investimento') ? $this->Html->link($indicadoresCarteira->carteiras_investimento->id, ['controller' => 'CarteirasInvestimentos', 'action' => 'view', $indicadoresCarteira->carteiras_investimento->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('Tipo Benchmark') ?></th>
                    <td><?= $indicadoresCarteira->has('tipo_benchmark') ? $this->Html->link($indicadoresCarteira->tipo_benchmark->id, ['controller' => 'TipoBenchmarks', 'action' => 'view', $indicadoresCarteira->tipo_benchmark->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('Periodo Meses') ?></th>
                    <td><?= $this->Number->format($indicadoresCarteira->periodo_meses) ?></td>
                </tr>
                <tr>
                    <th><?= __('Rentabilidade') ?></th>
                    <td><?= $this->Number->format($indicadoresCarteira->rentabilidade) ?></td>
                </tr>
                <tr>
                    <th><?= __('Desvio Padrao') ?></th>
                    <td><?= $this->Number->format($indicadoresCarteira->desvio_padrao) ?></td>
                </tr>
                <tr>
                    <th><?= __('Num Valores') ?></th>
                    <td><?= $this->Number->format($indicadoresCarteira->num_valores) ?></td>
                </tr>
                <tr>
                    <th><?= __('Rentab Min') ?></th>
                    <td><?= $this->Number->format($indicadoresCarteira->rentab_min) ?></td>
                </tr>
                <tr>
                    <th><?= __('Rentab Max') ?></th>
                    <td><?= $this->Number->format($indicadoresCarteira->rentab_max) ?></td>
                </tr>
                <tr>
                    <th><?= __('Max Drawdown') ?></th>
                    <td><?= $this->Number->format($indicadoresCarteira->max_drawdown) ?></td>
                </tr>
                <tr>
                    <th><?= __('Meses Acima Bench') ?></th>
                    <td><?= $this->Number->format($indicadoresCarteira->meses_acima_bench) ?></td>
                </tr>
                <tr>
                    <th><?= __('Sharpe') ?></th>
                    <td><?= $this->Number->format($indicadoresCarteira->sharpe) ?></td>
                </tr>
                <tr>
                    <th><?= __('Beta') ?></th>
                    <td><?= $this->Number->format($indicadoresCarteira->beta) ?></td>
                </tr>
                <tr>
                    <th><?= __('Data Final') ?></th>
                    <td><?= h($indicadoresCarteira->data_final) ?></td>
                </tr>
            </table>
        </div>
    </div>
</div>
