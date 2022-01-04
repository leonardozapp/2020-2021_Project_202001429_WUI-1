<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\RetornoRiscoFundo $retornoRiscoFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('Edit Retorno Risco Fundo'), ['action' => 'edit', $retornoRiscoFundo->cnpj_fundo_id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Retorno Risco Fundo'), ['action' => 'delete', $retornoRiscoFundo->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $retornoRiscoFundo->cnpj_fundo_id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Retorno Risco Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Retorno Risco Fundo'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="retornoRiscoFundos view content">
            <h3><?= h($retornoRiscoFundo->cnpj_fundo_id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Cnpj Fundo') ?></th>
                    <td><?= $retornoRiscoFundo->has('cnpj_fundo') ? $this->Html->link($retornoRiscoFundo->cnpj_fundo->id, ['controller' => 'CnpjFundos', 'action' => 'view', $retornoRiscoFundo->cnpj_fundo->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('Periodo Meses') ?></th>
                    <td><?= $this->Number->format($retornoRiscoFundo->periodo_meses) ?></td>
                </tr>
                <tr>
                    <th><?= __('Rentab Media') ?></th>
                    <td><?= $this->Number->format($retornoRiscoFundo->rentab_media) ?></td>
                </tr>
                <tr>
                    <th><?= __('Desvio Padrao') ?></th>
                    <td><?= $this->Number->format($retornoRiscoFundo->desvio_padrao) ?></td>
                </tr>
                <tr>
                    <th><?= __('Num Valores') ?></th>
                    <td><?= $this->Number->format($retornoRiscoFundo->num_valores) ?></td>
                </tr>
                <tr>
                    <th><?= __('Rentab Min') ?></th>
                    <td><?= $this->Number->format($retornoRiscoFundo->rentab_min) ?></td>
                </tr>
                <tr>
                    <th><?= __('Rentab Max') ?></th>
                    <td><?= $this->Number->format($retornoRiscoFundo->rentab_max) ?></td>
                </tr>
                <tr>
                    <th><?= __('Meses Abaixo Bench') ?></th>
                    <td><?= $this->Number->format($retornoRiscoFundo->meses_abaixo_bench) ?></td>
                </tr>
                <tr>
                    <th><?= __('Meses Acima Bench') ?></th>
                    <td><?= $this->Number->format($retornoRiscoFundo->meses_acima_bench) ?></td>
                </tr>
                <tr>
                    <th><?= __('Data Final') ?></th>
                    <td><?= h($retornoRiscoFundo->data_final) ?></td>
                </tr>
            </table>
        </div>
    </div>
</div>
