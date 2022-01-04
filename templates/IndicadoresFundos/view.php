<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\IndicadoresFundo $indicadoresFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('Edit Indicadores Fundo'), ['action' => 'edit', $indicadoresFundo->cnpj_fundo_id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Indicadores Fundo'), ['action' => 'delete', $indicadoresFundo->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $indicadoresFundo->cnpj_fundo_id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Indicadores Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Indicadores Fundo'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="indicadoresFundos view content">
            <h3><?= h($indicadoresFundo->cnpj_fundo_id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Cnpj Fundo') ?></th>
                    <td><?= $indicadoresFundo->has('cnpj_fundo') ? $this->Html->link($indicadoresFundo->cnpj_fundo->id, ['controller' => 'CnpjFundos', 'action' => 'view', $indicadoresFundo->cnpj_fundo->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('Tipo Benchmark') ?></th>
                    <td><?= $indicadoresFundo->has('tipo_benchmark') ? $this->Html->link($indicadoresFundo->tipo_benchmark->id, ['controller' => 'TipoBenchmarks', 'action' => 'view', $indicadoresFundo->tipo_benchmark->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('Periodo Meses') ?></th>
                    <td><?= $this->Number->format($indicadoresFundo->periodo_meses) ?></td>
                </tr>
                <tr>
                    <th><?= __('Rentabilidade') ?></th>
                    <td><?= $this->Number->format($indicadoresFundo->rentabilidade) ?></td>
                </tr>
                <tr>
                    <th><?= __('Desvio Padrao') ?></th>
                    <td><?= $this->Number->format($indicadoresFundo->desvio_padrao) ?></td>
                </tr>
                <tr>
                    <th><?= __('Num Valores') ?></th>
                    <td><?= $this->Number->format($indicadoresFundo->num_valores) ?></td>
                </tr>
                <tr>
                    <th><?= __('Rentab Min') ?></th>
                    <td><?= $this->Number->format($indicadoresFundo->rentab_min) ?></td>
                </tr>
                <tr>
                    <th><?= __('Rentab Max') ?></th>
                    <td><?= $this->Number->format($indicadoresFundo->rentab_max) ?></td>
                </tr>
                <tr>
                    <th><?= __('Max Drawdown') ?></th>
                    <td><?= $this->Number->format($indicadoresFundo->max_drawdown) ?></td>
                </tr>
                <tr>
                    <th><?= __('Meses Acima Bench') ?></th>
                    <td><?= $this->Number->format($indicadoresFundo->meses_acima_bench) ?></td>
                </tr>
                <tr>
                    <th><?= __('Sharpe') ?></th>
                    <td><?= $this->Number->format($indicadoresFundo->sharpe) ?></td>
                </tr>
                <tr>
                    <th><?= __('Beta') ?></th>
                    <td><?= $this->Number->format($indicadoresFundo->beta) ?></td>
                </tr>
                <tr>
                    <th><?= __('Data Final') ?></th>
                    <td><?= h($indicadoresFundo->data_final) ?></td>
                </tr>
            </table>
        </div>
    </div>
</div>
